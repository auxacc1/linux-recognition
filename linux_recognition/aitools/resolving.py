from asyncio import Semaphore
from logging import DEBUG, getLogger
from os import getenv
from textwrap import dedent

from asyncpg import Pool
from jinja2 import Environment
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from pydantic import BaseModel, Field

from linux_recognition.db.postgresql.licenses import fetch_identifiers, fetch_licenses, insert_licenses
from linux_recognition.log_management import get_error_details
from linux_recognition.typestore.datatypes import LicenseIdentifiers
from linux_recognition.typestore.errors import DatabaseError, LLMError, SQLTemplateError


logger = getLogger(__name__)


class ChatInteraction:

    def __init__(self, model: str, temperature: float = 0.01) -> None:
        self._llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            api_key=getenv('LINUX_RECOGNITION__OPENAI_API_KEY')
        )

    async def generate_formal_definition(self, text: str, software: str) -> str:
        text = text[:4096]
        prompt_template = ChatPromptTemplate.from_template(
            dedent(
                f'''
                Text:
                {{text}}
                
                Give a formal definition of {{software}} based on the above text related 
                to this software. Use maximum 2 sentences.'''
            ).strip()
        )

        class FormalDefinition(BaseModel):
            description: str = Field(description=f'A formal definition of {software}')

        response = await self._get_structured_response(text, software, prompt_template, FormalDefinition)
        return response.description

    async def extract_licenses(self, text: str, software: str) -> list[str]:
        text = text[:4096]
        prompt_template = ChatPromptTemplate.from_template(
            dedent(
                f'''
                Text:
                {{text}}
        
                Try, based on the above text describing how software {{software}} is licensed, 
                to identify names of all licenses under which this software is released. 
                The information returned should be accurate, in particular include the license 
                version if this can be inferred from the text.'''
            ).strip()
        )

        class LicenseExtractionResponse(BaseModel):
            licenses: list[str] = Field(description='A list of license names identified from the text')

        response = await self._get_structured_response(text, software, prompt_template, LicenseExtractionResponse)
        return response.licenses

    async def _get_structured_response[T: BaseModel](
            self,
            text: str,
            software: str,
            prompt_template: ChatPromptTemplate,
            response_model: type[T]
    ) -> T:
        llm = self._llm.with_structured_output(response_model)
        llm_chain = prompt_template | llm
        try:
            response = await llm_chain.ainvoke(
                {
                    'text': text,
                    'software': software
                }
            )
        except Exception as e:
            message = 'LLM interaction error'
            extra = get_error_details(e)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise LLMError(message) from e
        return response


class FaissLicenseResolver:

    def __init__(
            self,
            pool: Pool,
            environment: Environment,
            embeddings_model: str,
            semaphore: Semaphore,
            l2_threshold: float = 0.2
    ) -> None:
        self._pool: Pool = pool
        self._jinja_environment: Environment = environment
        self._semaphore: Semaphore = semaphore
        self._embeddings_model: str = embeddings_model
        self._l2_threshold: float = l2_threshold
        self._faiss: FAISS | None = None

    async def create_vectorstore(self) -> None:
        identifiers = await fetch_identifiers(self._pool, self._jinja_environment, self._semaphore)
        embedding = OpenAIEmbeddings(
            model=self._embeddings_model,
            api_key=getenv('LINUX_RECOGNITION__OPENAI_API_KEY')
        )
        try:
            self._faiss = await FAISS.afrom_texts(identifiers, embedding=embedding)
            logger.debug('Successfully created FAISS vectorstore for licenses')
        except Exception as e:
            message = 'Failed to create FAISS vectorstore for licenses'
            extra = get_error_details(e)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise LLMError(message) from e

    async def resolve(self, identifiers: list[str]) -> list[str]:
        try:
            identifiers_by_recognition = await self._classify_identifiers(identifiers)
        except Exception as e:
            message = 'Failed to resolve license names with FAISS'
            extra = get_error_details(e)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise LLMError(message) from e
        recognized_items = await fetch_licenses(
            self._pool, self._jinja_environment, identifiers_by_recognition.recognized, self._semaphore
        )
        if identifiers_by_recognition.unrecognized:
            try:
                await insert_licenses(
                    self._pool, self._jinja_environment, self._semaphore, identifiers_by_recognition.unrecognized
                )
            except (DatabaseError, SQLTemplateError):
                pass
        return [item.name for item in recognized_items] + identifiers_by_recognition.unrecognized

    async def _classify_identifiers(self, identifiers: list[str]) -> LicenseIdentifiers:
        license_identifiers = LicenseIdentifiers([], [])
        for identifier in identifiers:
            results = await self._faiss.asimilarity_search_with_score(identifier, k=1)
            license_document, l2_distance = results[0]
            if l2_distance <= self._l2_threshold:
                license_identifiers.recognized.append(license_document.page_content)
            else:
                license_identifiers.unrecognized.append(identifier)
        return license_identifiers
