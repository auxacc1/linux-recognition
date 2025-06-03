from asyncio import run
from logging import DEBUG

from configuration import get_project_directory, initialize_settings, is_initialized, mark_initialized
from context import managed_context, prepare_context
from db.postgresql.alpine import create_alpine_packages_table, update_alpine_packages_table
from db.postgresql.cpe import create_cpe_entities, populate_cpe_entities
from db.postgresql.licenses import create_licenses_table, populate_licenses_table
from db.postgresql.output import create_output_table
from db.postgresql.repology import rebuild_repology_database
from log_management import get_error_details, init_logging
from typestore.datatypes import RecognitionContext
from typestore.errors import LinuxRecognitionError
from webtools.download import download_cpe_dictionary, download_spdx_licenses


def initialize() -> None:
    run(_initialize())


async def _initialize() -> None:
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
        logger.info('Initialization started')
        context = await prepare_context(
            project_directory, settings, create_licenses_vectorstore=False
        )
        async with managed_context(context) as recognition_context:
            try:
                await _prepare_databases(recognition_context)
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


async def _prepare_databases(recognition_context: RecognitionContext) -> None:
    # currently only partially parallelized
    recognition_context: RecognitionContext
    session_manager = recognition_context.session_handler
    semaphore = recognition_context.synchronization.semaphore
    project_directory = recognition_context.project_directory

    await create_output_table(recognition_context.recognized_db_pool, project_directory)

    await rebuild_repology_database(recognition_context.source_db_pools.repology, project_directory)

    packages_pool = recognition_context.source_db_pools.packages
    await create_alpine_packages_table(packages_pool, project_directory)
    await update_alpine_packages_table(packages_pool, project_directory, session_manager, semaphore)

    downloads_directory = project_directory / 'data' / 'downloaded'
    await download_cpe_dictionary(recognition_context.session_handler, downloads_directory, semaphore)
    await create_cpe_entities(packages_pool, project_directory)
    await populate_cpe_entities(packages_pool, project_directory)
    await download_spdx_licenses(recognition_context.session_handler, downloads_directory, semaphore)
    await create_licenses_table(packages_pool, project_directory)
    await populate_licenses_table(packages_pool, project_directory)
