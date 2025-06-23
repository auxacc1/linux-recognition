from logging import getLogger

import pytest
from pytest_asyncio import is_async_test

from linux_recognition.configuration import get_project_directory, initialize_settings
from linux_recognition.context import managed_context, prepare_context
from linux_recognition.initialization import is_initialized
from linux_recognition.typestore.errors import ProjectNotInitializedError


logger = getLogger(__name__)


def pytest_collection_modifyitems(items):
    pytest_asyncio_tests = (item for item in items if is_async_test(item))
    session_scope_marker = pytest.mark.asyncio(loop_scope="session")
    for async_test in pytest_asyncio_tests:
        async_test.add_marker(session_scope_marker, append=False)


@pytest.fixture(scope='class')
async def recognition_context():
    project_initialized = await is_initialized()
    if not project_initialized:
        logger.critical('The project must be initialized before the tests can be run')
        raise ProjectNotInitializedError()
    project_directory = await get_project_directory()
    settings = initialize_settings(project_directory)
    context = await prepare_context(project_directory, settings, create_licenses_vectorstore=False)
    async with managed_context(context) as recognition_ctx:
        yield recognition_ctx
