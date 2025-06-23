from asyncio import create_task, gather, Semaphore
from logging import getLogger
from anyio import Path
from uuid import uuid4

import aiofile

from linux_recognition.log_management import get_error_details
from linux_recognition.typestore.datatypes import SessionHandler
from linux_recognition.typestore.errors import DataDependencyError, ResponseError
from linux_recognition.webtools.response import BinaryResponse


logger = getLogger(__name__)


async def download_repology_database_dump(
        session_manager: SessionHandler,
        downloads_directory: Path,
        semaphore: Semaphore,
        output_name: str = 'repology_dump',
):
    url = 'https://dumps.repology.org/repology-database-dump-latest.sql.zst'
    extensions = '.sql.zst'
    await _fetch_file_safe(
        url,
        session_manager,
        downloads_directory,
        output_name,
        semaphore=semaphore,
        extensions=extensions
    )



async def download_apkindex_files(
        session_manager: SessionHandler,
        downloads_directory: Path,
        semaphore: Semaphore
) -> tuple[str | None, ...]:
    base_path = 'https://dl-cdn.alpinelinux.org/'
    branch_paths = {'release': 'alpine/latest-stable/', 'edge': 'alpine/edge/'}
    repositories = {'release': ['main', 'community'], 'edge': ['testing']}
    architecture_path = '/x86_64/'
    apkindex_path = 'APKINDEX.tar.gz'
    apkindex_files = [{
        'url': base_path + branch_paths[branch_type] + repo + architecture_path + apkindex_path,
        'file_name': f'apkindex_{repo}'
        } for branch_type in branch_paths for repo in repositories[branch_type]
    ]
    aws = [
        create_task(
            _fetch_file(
                file['url'],
                session_manager,
                downloads_directory,
                file['file_name'],
                semaphore=semaphore,
                extensions='.tar.gz'
            ),
            name=str(uuid4())
        )
        for file in apkindex_files
    ]
    return tuple(await gather(*aws))


async def download_cpe_dictionary(
        session_manager: SessionHandler,
        downloads_directory: Path,
        semaphore: Semaphore
) -> None:
    url = 'https://nvd.nist.gov/feeds/xml/cpe/dictionary/official-cpe-dictionary_v2.3.xml.zip'
    output_file_name = 'cpe_dictionary'
    extensions = '.xml.zip'
    await _fetch_file_safe(
        url,
        session_manager,
        downloads_directory,
        output_file_name,
        semaphore=semaphore,
        extensions=extensions
    )


async def download_spdx_licenses(
        session_manager: SessionHandler,
        downloads_directory: Path,
        semaphore: Semaphore
) -> None:
    session_name = 'github'
    url = 'https://raw.githubusercontent.com/spdx/license-list-data/refs/heads/main/json/licenses.json'
    output_file_name = 'licenses'
    extensions = '.json'
    await _fetch_file(
        url,
        session_manager,
        downloads_directory,
        output_file_name,
        semaphore=semaphore,
        extensions=extensions,
        session_name=session_name
    )


async def _fetch_file(
        url: str,
        session_manager: SessionHandler,
        downloads_directory: Path,
        output_file_name: str,
        semaphore: Semaphore | None = None,
        extensions: str = '',
        session_name: str = 'common'
) -> str | None:
    output_filename_with_extensions = output_file_name + extensions
    try:
        response = await BinaryResponse(
            url,
            session_manager,
            semaphore=semaphore,
            session_name=session_name
        ).fetch()
    except ResponseError:
        message = 'Download error'
        extra = {'file_name': output_filename_with_extensions}
        logger.error(message, extra=extra)
        raise
    content = response.get_content()
    save_path = str(downloads_directory.joinpath(output_filename_with_extensions))
    try:
        async with aiofile.async_open(save_path, mode='wb') as outfile:
            await outfile.write(content)
    except OSError as e:
        message = 'Failed to save downloaded file'
        extra = get_error_details(e)
        extra['file_name'] = output_filename_with_extensions
        logger.error(message, extra=extra)
        raise DataDependencyError() from e
    logger.info('Successful download', extra={'file_name': output_filename_with_extensions})
    return output_file_name


async def _fetch_file_safe(
        url: str,
        session_manager: SessionHandler,
        downloads_directory: Path,
        output_file_name: str,
        semaphore: Semaphore | None = None,
        extensions: str = '',
        session_name: str = 'common'
) -> str | None:
    try:
        return await _fetch_file(
            url,
            session_manager,
            downloads_directory,
            output_file_name,
            semaphore=semaphore,
            extensions=extensions,
            session_name=session_name
        )
    except (DataDependencyError, ResponseError):
        return None
