import re
from asyncio import Semaphore
from logging import getLogger, DEBUG
from typing import Any

from anyio import Path
from jinja2 import Environment, FileSystemLoader, Template, TemplateError
from jinja2.filters import FILTERS

from log_management import get_error_details
from synchronization import async_to_thread
from typestore.errors import SQLTemplateError


logger = getLogger(__name__)


async def render(
        environment: Environment,
        query_file: str,
        semaphore,
        **context_kwargs: Any
) -> str:
    template = await _get_template(environment, query_file, semaphore)
    z =  await template.render_async(**context_kwargs)
    return z

async def create_jinja_environment(project_directory: Path, semaphore: Semaphore) -> Environment:
    try:
        return await async_to_thread(semaphore, _create_jinja_environment, project_directory)
    except TemplateError as e:
        message = 'Failed to create jinja environment'
        extra = get_error_details(e)
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise SQLTemplateError() from e


def _create_jinja_environment(project_directory: Path) -> Environment:
    template_directory = project_directory / 'db' / 'sql' / 'postgresql'
    environment = Environment(
        loader=FileSystemLoader(template_directory),
        enable_async=True
    )
    environment.filters.update(FILTERS)
    environment.filters.update(identifier=_identifier_filter)
    return environment


async def _get_template(environment: Environment, query_file: str, semaphore: Semaphore) -> Template:
    return await async_to_thread(semaphore, environment.get_template, query_file)


def _identifier_filter(identifier: str) -> str:
    if not re.match(r'^[^\W\d]\w*$', identifier):
        raise ValueError(f'Invalid identifier: {identifier}')
    return _quote_identifier(identifier)


def _quote_identifier(identifier: str) -> str:
    return '"{}"'.format(identifier.replace('"', '""'))
