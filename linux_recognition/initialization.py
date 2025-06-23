from asyncio import run, SelectorEventLoop, Semaphore
from logging import DEBUG, Logger
from platform import system

from anyio import Path
from asyncpg import Pool
from jinja2 import Environment

from linux_recognition.configuration import get_project_directory, initialize_settings, Settings
from linux_recognition.context import managed_context, prepare_context
from linux_recognition.db.postgresql.alpine import create_alpine_packages_table, update_alpine_packages_table
from linux_recognition.db.postgresql.core import create_database
from linux_recognition.db.postgresql.cpe import create_cpe_entities, populate_cpe_entities
from linux_recognition.db.postgresql.licenses import create_licenses_table, populate_licenses_table
from linux_recognition.db.postgresql.output import create_output_table
from linux_recognition.db.postgresql.repology import (
    rebuild_repology_database, decompress_repology_database_dump, restore_repology_origin_database
)
from linux_recognition.log_management import get_error_details, init_logging
from linux_recognition.typestore.datatypes import RecognitionContext, SessionHandler
from linux_recognition.typestore.errors import LinuxRecognitionError
from linux_recognition.webtools.download import (
    download_cpe_dictionary, download_repology_database_dump, download_spdx_licenses
)


def initialize() -> None:
    run(_prepare_initialization_environment(), loop_factory=SelectorEventLoop)
    run(_initialize())


async def is_initialized() -> bool:
    data_directory = await _get_data_directory()
    return await (data_directory / 'initialized').exists()


async def mark_initialized():
    data_directory = await _get_data_directory()
    await data_directory.mkdir(parents=True, exist_ok=True)
    await (data_directory / 'initialized').touch(exist_ok=True)


async def _prepare_initialization_environment() -> None:
    project_directory = await get_project_directory()
    settings = initialize_settings(project_directory)
    logger, listener = init_logging(settings.logging, project_directory)
    with listener.started():
        try:
            initialized = await is_initialized()
        except OSError as e:
            message = 'Failed to check the initialization flag'
            extra = get_error_details(e)
            logger.critical(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise
        if initialized:
            logger.warning('Project already initialized')
            return
        await _create_databases(settings, logger)


async def _create_databases(settings: Settings, logger: Logger) -> None:
    core_databases = settings.database.core_databases
    for database in core_databases:
        postgres_config = settings.database.postgres_default.for_database(database)
        await create_database(postgres_config)
    logger.info('Databases successfully created')


async def _initialize() -> None:
    project_directory = await get_project_directory()
    settings = initialize_settings(project_directory)
    logger, listener = init_logging(settings.logging, project_directory)
    with listener.started():
        context = await prepare_context(
            project_directory, settings, create_licenses_vectorstore=False
        )
        async with managed_context(context) as recognition_context:
            try:
                await _populate_initial_data(recognition_context, settings)
            except LinuxRecognitionError:
                message = 'Databases initialization failed'
                logger.critical(message, exc_info=logger.isEnabledFor(DEBUG))
                raise
            logger.info('Initialization completed')
            try:
                await mark_initialized()
            except OSError as e:
                message = 'Failed to set the initialization flag'
                extra = get_error_details(e)
                logger.critical(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)


async def _populate_initial_data(recognition_context: RecognitionContext, settings: Settings) -> None:
    # currently only partially parallelized

    session_manager = recognition_context.session_handler
    jinja_environment = recognition_context.jinja_environment
    semaphore = recognition_context.synchronization.semaphore
    recognized_pool = recognition_context.recognized_db_pool
    repology_pool = recognition_context.source_db_pools.repology
    packages_pool = recognition_context.source_db_pools.packages
    project_directory = recognition_context.project_directory
    downloads_directory = project_directory / 'data' / 'downloaded'

    await _build_repology_origin_database(
        session_manager,
        repology_pool,
        jinja_environment,
        project_directory,
        semaphore,
        settings
    )
    await rebuild_repology_database(repology_pool, jinja_environment, semaphore)

    await create_alpine_packages_table(packages_pool, jinja_environment, semaphore)
    await update_alpine_packages_table(packages_pool, jinja_environment, project_directory, session_manager, semaphore)

    await download_cpe_dictionary(recognition_context.session_handler, downloads_directory, semaphore)
    await create_cpe_entities(packages_pool, jinja_environment, semaphore)
    await populate_cpe_entities(packages_pool, jinja_environment, project_directory, semaphore)

    await download_spdx_licenses(recognition_context.session_handler, downloads_directory, semaphore)
    await create_licenses_table(packages_pool, jinja_environment, semaphore)
    await populate_licenses_table(packages_pool, jinja_environment, project_directory, semaphore)

    await create_output_table(recognized_pool, jinja_environment, semaphore)


async def _build_repology_origin_database(
        session_manager: SessionHandler,
        pool: Pool,
        environment: Environment,
        project_directory: Path,
        semaphore: Semaphore,
        settings: Settings
) -> None:
    downloads_directory = project_directory / 'data' / 'downloaded'
    dump_name = 'repology_dump'
    compressed_dump_name = f'{dump_name}.sql.zst'
    decompressed_dump_name = f'{dump_name}.sql'
    postgres_config = settings.database.postgres_default.for_database(settings.database.core_databases.repology)
    psql_directory = settings.database.psql_directory
    await download_repology_database_dump(session_manager, downloads_directory, semaphore, dump_name)
    await decompress_repology_database_dump(compressed_dump_name, decompressed_dump_name, project_directory, semaphore)
    await restore_repology_origin_database(
        pool,
        environment,
        project_directory,
        decompressed_dump_name,
        semaphore,
        postgres_config,
        psql_directory=psql_directory
    )


async def _get_data_directory() -> Path:
    system_used = system()
    if system_used == 'Windows':
        data_directory = await Path.home() / 'AppData' / 'Local' / 'linux_recognition'
    elif system_used == 'Linux':
        data_directory = await Path.home() / '.local' / 'share' / 'linux_recognition'
    else:
        project_directory = await get_project_directory()
        data_directory = project_directory.parent  / '.linux_recognition'
    return data_directory
