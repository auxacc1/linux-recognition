from asyncio import create_task, gather
from itertools import batched
from logging import DEBUG, Logger
from uuid import uuid4

from configuration import get_project_directory, initialize_settings, is_initialized
from context import managed_context, prepare_context
from db.postgresql.output import filter_recognized_fingerprints, update_recognized_table
from log_management import get_error_details, init_logging
from normalization import normalize_fingerprints
from software import SoftwareRecognizer
from typestore.datatypes import (
    Fingerprint,
    FingerprintDict,
    VersionNormalizationPatterns,
    RecognitionContext,
    RecognitionResult
)
from typestore.errors import DatabaseError, ProjectNotInitializedError, SQLTemplateError


async def recognize(raw_fingerprints: list[FingerprintDict], segment_length: int = 20) -> None:
    project_directory = await get_project_directory()
    settings = initialize_settings(project_directory)
    logger, listener = init_logging(settings.logging, project_directory)
    with listener.started():
        try:
            project_initialized = await is_initialized()
        except OSError as e:
            message = 'Failed to check the initialization flag'
            extra = get_error_details(e)
            logger.critical(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            raise
        if not project_initialized:
            message = 'The project must be initialized before linux_recognition can run'
            project_not_initialized_error = ProjectNotInitializedError()
            extra = get_error_details(project_not_initialized_error)
            logger.critical(message, extra=extra)
            raise project_not_initialized_error
        context = await prepare_context(
            project_directory, settings, create_licenses_vectorstore=False
        )
        async with managed_context(context) as recognition_context:
            logger.info('Start of recognition')
            recognition_context: RecognitionContext
            normalization_patterns = VersionNormalizationPatterns()
            fingerprints = normalize_fingerprints(raw_fingerprints, normalization_patterns)
            recognized_db_pool = recognition_context.recognized_db_pool
            try:
                fingerprints = await filter_recognized_fingerprints(
                    recognized_db_pool, project_directory, fingerprints
                )
            except (DatabaseError, SQLTemplateError):
                message = 'Failed to filter out fingerprints'
                logger.critical(message, exc_info=logger.isEnabledFor(DEBUG))
                raise
            for segment in batched(fingerprints, segment_length):
                await _recognize_segment(
                    segment=segment,
                    recognition_context=recognition_context,
                    logger=logger
            )


async def _recognize_segment(
        segment: tuple[Fingerprint, ...],
        recognition_context: RecognitionContext,
        logger: Logger
) -> None:
    tasks = [
        create_task(_recognize(fp, recognition_context), name=str(uuid4())) for fp in segment
    ]
    segment_outcome: tuple[RecognitionResult | None, ...] = tuple(await gather(*tasks))
    results = [item for item in segment_outcome if item is not None]
    if results:
        output_db_pool = recognition_context.recognized_db_pool
        project_directory = recognition_context.project_directory
        try:
            await update_recognized_table(output_db_pool, project_directory, results)
        except (DatabaseError, SQLTemplateError):
            logger.critical(
                'Database upsert failed for the current batch, subsequent batches will likely fail'
            )


async def _recognize(
        fingerprint: Fingerprint,
        recognition_context: RecognitionContext
) -> RecognitionResult | None:
    recognized = await SoftwareRecognizer(fingerprint, recognition_context).recognize()
    if recognized is None:
        return recognized
    return RecognitionResult(
        fingerprint=fingerprint,
        software=recognized.software,
        publisher=recognized.publisher,
        description=recognized.description,
        licenses=recognized.licenses,
        homepage=recognized.homepage,
        version=recognized.version,
        release_date=recognized.release_date,
        cpe_string=recognized.cpe_string,
        unspsc=recognized.unspsc
    )
