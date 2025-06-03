from collections.abc import Callable, Coroutine
from logging import getLogger, DEBUG
from typing import Any

from anyio import Path
from asyncpg import Connection, create_pool, Pool

from configuration import PostgresConfig
from db.rendering import render
from log_management import get_error_details
from typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


async def query_db[T](
        pool: Pool,
        query_fn: Callable[[Connection, str], Coroutine[Any, Any, T]],
        query_file: str,
        project_directory: Path,
        **context_kwargs: Any
) -> T:
    sql_directory = project_directory / 'db' / 'sql' / 'postgresql'
    try:
        query = await render(query_file, sql_directory, **context_kwargs)
    except Exception as e:
        message = 'SQL template error'
        extra = get_error_details(e)
        extra['db_system'] = 'PostgreSQL'
        extra['query_file'] = query_file
        if context_kwargs:
            extra.update(context_kwargs)
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise SQLTemplateError() from e
    async with pool.acquire() as connection:
        try:
            result = await query_fn(connection, query)
        except Exception as e:
            message = 'Database query error'
            extra = get_error_details(e)
            extra['db_system'] = 'PostgreSQL'
            extra['query_file'] =  query_file
            if context_kwargs:
                extra.update(context_kwargs)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise DatabaseError() from e
    return result


async def init_pool(postgres_config: PostgresConfig) -> Pool:
    try:
        return await create_pool(
            database=postgres_config.dbname,
            user=postgres_config.user,
            password=postgres_config.password,
            host=postgres_config.host,
            port=postgres_config.port
        )
    except Exception as e:
        message = 'Unsuccessful pool creation for database'
        extra = get_error_details(e)
        extra['db_system'] = 'PostgreSQL'
        extra['database'] = postgres_config.dbname
        logger.error(message, extra=extra)
        raise DatabaseError() from e
