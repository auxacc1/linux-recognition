from collections.abc import Callable

import pytest
from anyio import Path
from asyncpg import Pool

from configuration import get_project_directory, initialize_settings
from context import managed_context, prepare_context
from typestore.datatypes import (
    DatePatterns,
    LibraryPatterns,
    LicenseResolver,
    RecognitionContext,
    SourceDbPools,
    SynchronizationPrimitives,
    SessionHandler
)


@pytest.mark.asyncio
async def test_prepare_context():
    project_directory = await get_project_directory()
    settings = initialize_settings(project_directory)
    context = await prepare_context(
        project_directory, settings, create_licenses_vectorstore=True
    )
    async with managed_context(context) as recognition_ctx:
        assert isinstance(recognition_ctx, RecognitionContext)
        assert isinstance(recognition_ctx.project_directory, Path)
        assert isinstance(recognition_ctx.session_handler, SessionHandler)
        assert isinstance(recognition_ctx.output_db_pool, Pool)
        assert isinstance(recognition_ctx.source_db_pools, SourceDbPools)
        for pool in recognition_ctx.source_db_pools:
            assert isinstance(pool, Pool)
        assert isinstance(recognition_ctx.license_resolver, LicenseResolver)
        assert isinstance(recognition_ctx.is_host_supported, Callable)
        assert isinstance(recognition_ctx.date_patterns, DatePatterns)
        assert isinstance(recognition_ctx.library_patterns, LibraryPatterns)
        assert isinstance(recognition_ctx.synchronization, SynchronizationPrimitives)
