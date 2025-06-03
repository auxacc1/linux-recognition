from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from logging import getLogger, DEBUG
from pathlib import Path

from aiohttp import ClientError

from aitools.resolving import ChatInteraction, FaissLicenseResolver
from configuration import DatabaseSettings, Settings
from db.postgresql.core import init_pool, Pool
from log_management import get_error_details
from reposcan.dateparse import generate_complete_date_patterns, generate_no_year_patterns
from reposcan.projects import is_host_supported
from typestore.datatypes import (
    DatePatterns,
    DbPools,
    LibraryPatterns,
    RecognitionContext,
    SourceDbPools,
    SynchronizationPrimitives
)
from typestore.errors import ContextPreparationError, DatabaseError, LLMError, SQLTemplateError
from webtools.session import SessionManager


logger = getLogger(__name__)


@asynccontextmanager
async def managed_context(
        recognition_context: RecognitionContext
) -> AsyncGenerator[RecognitionContext, None]:
    try:
        yield recognition_context
    finally:
        await recognition_context.session_handler.close_sessions()
        await _close_pools([recognition_context.recognized_db_pool, *recognition_context.source_db_pools])


async def prepare_context(
        project_directory: Path,
        settings: Settings,
        create_licenses_vectorstore
) -> RecognitionContext:
    db_pools = await _init_db_pools(settings.database)
    llm_interaction = ChatInteraction(model=settings.openai.chat)
    license_resolver = FaissLicenseResolver(
        db_pools.source.packages,
        project_directory,
        embeddings_model=settings.openai.embeddings
    )
    if create_licenses_vectorstore:
        try:
            await license_resolver.create_vectorstore()
        except (DatabaseError, LLMError, SQLTemplateError) as e:
            await _close_pools([db_pools.recognized, *db_pools.source])
            raise ContextPreparationError() from e
    try:
        session_manager = SessionManager()
    except ClientError as e:
        message = 'Failed to initialize session manager'
        extra = get_error_details(e)
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        await _close_pools([db_pools.recognized, *db_pools.source])
        raise ContextPreparationError() from e
    recognized_db_pool = db_pools.recognized
    source_db_pools = db_pools.source
    date_patterns_full = generate_complete_date_patterns()
    date_patterns_no_year = generate_no_year_patterns()
    date_patterns = DatePatterns(date_patterns_full, date_patterns_no_year)
    return RecognitionContext(
        project_directory=project_directory,
        session_handler=session_manager,
        recognized_db_pool=recognized_db_pool,
        source_db_pools=source_db_pools,
        llm_interaction=llm_interaction,
        license_resolver=license_resolver,
        is_host_supported=is_host_supported,
        date_patterns=date_patterns,
        library_patterns=LibraryPatterns(),
        synchronization=SynchronizationPrimitives.create()
    )


async def _init_db_pools(database_settings: DatabaseSettings) -> DbPools:
    recognized_db = database_settings.recognized_db
    repology_db = database_settings.repology_db
    packages_db = database_settings.packages_db
    core_databases = [recognized_db, repology_db, packages_db]
    default_config = database_settings.postgres_default
    pools = {}
    for db in core_databases:
        try:
            pools[db] = await init_pool(default_config.for_database(db))
        except DatabaseError:
            await _close_pools(list(pools.values()))
            raise
    udd_config = database_settings.postgres_udd
    try:
        pools[udd_config.dbname] = await init_pool(udd_config)
    except DatabaseError:
        await _close_pools(list(pools.values()))
        raise
    recognized_db_pool = pools[database_settings.recognized_db]
    source_db_pools = SourceDbPools(
        repology=pools[repology_db],
        packages=pools[packages_db],
        udd=pools[udd_config.dbname]
    )
    return DbPools(recognized_db_pool, source_db_pools)


async def _close_pools(pools: list[Pool]) -> None:
    for pool in pools:
        if isinstance(pool, Pool):
            await pool.close()
