import re
from typing import Any

from jinja2_async_environment.environment import AsyncEnvironment
from jinja2_async_environment.loaders import AsyncFileSystemLoader
from anyio import Path


async def render(query_file: str, sql_directory: Path, **context_kwargs: Any) -> str:
    environment = AsyncEnvironment(
        loader=AsyncFileSystemLoader(sql_directory),
        autoescape=False,
        enable_async=True
    )
    environment.filters['identifier'] = _identifier_filter
    template = await environment.get_template_async(query_file)
    return await template.render_async(**context_kwargs)


def _identifier_filter(identifier: str) -> str:
    if not re.match(r'^[^\W\d]\w*$', identifier):
        raise ValueError(f'Invalid identifier: {identifier}')
    return _quote_identifier(identifier)


def _quote_identifier(identifier: str) -> str:
    return '"{}"'.format(identifier.replace('"', '""'))
