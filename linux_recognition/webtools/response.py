import re
from abc import ABC, abstractmethod
from asyncio import Semaphore
from collections.abc import Mapping
from logging import getLogger, DEBUG
from typing import Any, Self
from urllib.parse import urljoin

from aiohttp import ClientResponse, ClientResponseError
from bs4 import BeautifulSoup
from html2text import HTML2Text
from yarl import URL

from linux_recognition.log_management import get_error_details
from linux_recognition.synchronization import async_to_thread
from linux_recognition.typestore.datatypes import HTMLParse, SessionHandler
from linux_recognition.typestore.errors import ResponseError
from linux_recognition.webtools.session import get_headers


logger = getLogger(__name__)


class Response(ABC):

    def __init__(
            self,
            url: str | URL,
            session_manager: SessionHandler,
            session_name: str = 'common',
            headers: Mapping = None,
            params: Mapping = None,
            treat_http_client_error_as_warning: bool = False,
            timeout: int = 300,
            semaphore: Semaphore | None = None
    ) -> None:
        self._url: URL = url if isinstance(url, URL) else URL(url)
        self._url_pre_redirect = self._url
        self._session = session_manager.get_session(session_name)
        self._headers = headers if headers is not None else get_headers(session_name)
        self._params = params
        self._treat_http_client_error_as_warning = treat_http_client_error_as_warning
        self._timeout = timeout
        self._semaphore = semaphore if semaphore is not None else Semaphore()
        self._content = None
        self._status_code: int | None = None

    async def fetch(self) -> Self:
        if not self._url:
            return self
        await self._perform_fetch()
        return self

    def get_url(self) -> str:
        return str(self._url)

    def get_content(self) -> Any:
        return self._content

    async def _perform_fetch(self) ->  None:
        try:
            response = await self._fetch()
            self._status_code = response.status
            logger.info(
                'HTTP request',
                extra={
                    'url': self._url,
                    'status_code': self._status_code
                }
            )
            response.raise_for_status()
        except ClientResponseError as e:
            self._content = None
            message, extra = self._get_error_details(e)
            if self._treat_http_client_error_as_warning and 400 <= e.status <= 499:
                logger.warning(message, extra=extra)
            else:
                logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise ResponseError(message) from e
        except Exception as e:
            self._content = None
            message, extra = self._get_error_details(e)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise ResponseError(message) from e

    @abstractmethod
    async def _fetch(self) -> ClientResponse:
        pass

    def _get_error_details(self, error: Exception, message: str = 'Response Error'):
        extra = get_error_details(error)
        extra['url'] = self._url
        extra['url_pre_redirect'] = self._url_pre_redirect
        extra['status_code'] = self._status_code
        return message, extra


class BinaryResponse(Response):

    def __init__(
            self,
            url: str | URL,
            session_manager: SessionHandler,
            session_name: str = 'common',
            headers: Mapping = None,
            params: Mapping = None,
            treat_http_client_error_as_warning: bool = False,
            timeout: int = 300,
            semaphore: Semaphore | None = None
    ) -> None:
        super().__init__(
            url,
            session_manager=session_manager,
            session_name=session_name,
            headers=headers,
            params=params,
            treat_http_client_error_as_warning=treat_http_client_error_as_warning,
            timeout=timeout,
            semaphore=semaphore
        )

    def get_content(self) -> bytes | None:
        return self._content

    async def _fetch(self) -> ClientResponse:
        async with self._session.get(
                self._url,
                headers=self._headers,
                params=self._params,
                timeout=self._timeout
        ) as response:
            self._url = response.url
            self._content = await response.read()
            return response


class JsonResponse(Response):

    def __init__(
            self,
            url: str | URL,
            session_manager: SessionHandler,
            session_name: str = 'common',
            headers: Mapping = None,
            params: Mapping = None,
            treat_http_client_error_as_warning: bool = False,
            timeout: int = 300,
            semaphore: Semaphore | None = None
    ) -> None:
        super().__init__(
            url,
            session_manager=session_manager,
            session_name=session_name,
            headers=headers,
            params=params,
            treat_http_client_error_as_warning=treat_http_client_error_as_warning,
            timeout=timeout,
            semaphore=semaphore
        )

    def get_content(self) -> Mapping | None:
        return self._content

    async def _fetch(self) -> ClientResponse:
        async with self._session.get(
                self._url,
                headers=self._headers,
                params=self._params,
                timeout=self._timeout
        ) as response:
            self._url = response.url
            self._content = await response.json()
            return response


class TextResponse(Response):

    def __init__(
            self,
            url: str | URL,
            session_manager: SessionHandler,
            session_name: str = 'common',
            headers: Mapping = None,
            params: Mapping = None,
            treat_http_client_error_as_warning: bool = False,
            timeout: int = 300,
            semaphore: Semaphore | None = None,
            no_meta_refresh: bool = True
    ) -> None:
        super().__init__(
            url,
            session_manager=session_manager,
            session_name=session_name,
            headers=headers,
            params=params,
            treat_http_client_error_as_warning=treat_http_client_error_as_warning,
            timeout=timeout,
            semaphore=semaphore
        )
        self._no_meta_refresh = no_meta_refresh

    def get_content(self) -> str | None:
        return self._content

    async def _fetch(self) -> ClientResponse:
        async with self._session.get(
            self._url,
            headers=self._headers,
            params=self._params,
            timeout=self._timeout
        ) as response:
            self._url = response.url
            try:
                self._content = await response.text()
            except UnicodeError:
                self._content = await response.text(encoding='latin-1')
            if self._no_meta_refresh:
                return response
        new_url = await async_to_thread(self._semaphore, self._extract_meta_refresh_url)
        if new_url is None:
            return response
        return await self._fetch()

    def _extract_meta_refresh_url(self) -> str | None:
        soup = BeautifulSoup(self._content, features='html.parser')
        meta = soup.find(
            lambda tag: tag.name == 'meta'
                        and tag.get('http-equiv', '').lower() == 'refresh'
                        and tag.get('content', '')
        )
        if meta is None:
            return None
        new_url_match = re.search(r'url=(\S+)', meta['content'], re.IGNORECASE)
        if new_url_match is None:
            return None
        new_url_part = new_url_match.group(1)
        response_url_hostname = self._url.host
        if response_url_hostname is None:
            return None
        scheme = self._url.scheme
        new_url = urljoin(f'{scheme}//{response_url_hostname}', new_url_part)
        return new_url

async def fetch_html_text(
        url: str,
        session_manager: SessionHandler,
        no_meta_refresh: bool = True,
        semaphore: Semaphore = None
) -> HTMLParse:
    if semaphore is None:
        semaphore = Semaphore()
    try:
        response = await TextResponse(
            url,
            session_manager=session_manager,
            semaphore=semaphore,
            no_meta_refresh=no_meta_refresh
        ).fetch()
    except ResponseError:
        return HTMLParse()
    response_text = response.get_content()
    response_url = response.get_url()
    return await async_to_thread(semaphore, _parse_response, response_text, response_url)


def _parse_response(response_text: str, response_url: str) -> HTMLParse:
    h = HTML2Text()
    h.ignore_links = True
    h.ignore_mailto_links = True
    h.images_to_alt = True
    h.ignore_tables = False
    h.single_line_break = True
    try:
        parsed = h.handle(response_text)
    except Exception as e:
        message = 'HTML2Text error'
        extra = get_error_details(e)
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise ResponseError(message) from e
    parsed = _remove_list_items(parsed)
    html_parse = HTMLParse(parsed=parsed, raw=response_text, url=response_url)
    return html_parse


def _remove_list_items(text: str) -> str:
    if not text:
        return ''
    pattern = r'^\*'
    text_lines = text.splitlines()
    relevant_lines = [
        line.strip() for line in text_lines if line.strip() and re.search(pattern, line.strip()) is None
    ]
    return ' '.join(relevant_lines)
