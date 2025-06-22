from asyncio import Semaphore
from collections.abc import Callable, Coroutine
from logging import getLogger, DEBUG
from typing import Any

from asyncpg import Connection, create_pool, Pool, PostgresError
from jinja2 import Environment
from psycopg import AsyncConnection
from psycopg.errors import Error
from psycopg.sql import SQL, Identifier

from configuration import PostgresConfig
from db.rendering import render
from log_management import get_error_details
from typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


async def query_db[T](
        pool: Pool,
        environment: Environment,
        query_fn: Callable[[Connection, str], Coroutine[Any, Any, T]],
        query_file: str,
        semaphore: Semaphore,
        **context_kwargs: Any
) -> T:
    try:
        query = await render(environment, query_file, semaphore, **context_kwargs)
    except Exception as e:
        message = 'SQL template error'
        extra = get_error_details(e)
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
    except PostgresError as e:
        message = 'Unsuccessful pool creation for database'
        extra = get_error_details(e)
        extra['database'] = postgres_config.dbname
        logger.error(message, extra=extra)
        raise DatabaseError() from e


async def create_database(postgres_config: PostgresConfig) -> None:
    async with await AsyncConnection.connect(
            user=postgres_config.user,
            password=postgres_config.password,
            dbname='template1',
            host=postgres_config.host,
            port=postgres_config.port,
            autocommit=True
    ) as connection:
        dbname = postgres_config.dbname
        query = SQL('CREATE DATABASE {}').format(Identifier(dbname))
        async with connection.cursor() as cursor:
            try:
                await cursor.execute(query)
            except Error as e:
                extra = get_error_details(e)
                extra['database'] = dbname
                logger.error(
                    'Failed to create the database', exc_info=logger.isEnabledFor(DEBUG), extra=extra
                )
                raise DatabaseError() from e