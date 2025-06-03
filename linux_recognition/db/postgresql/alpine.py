import os
import re
import tarfile
from asyncio import create_task, gather, Semaphore
from collections import ChainMap
from collections.abc import Iterable
from collections.abc import Mapping
from logging import DEBUG, getLogger
from uuid import uuid4

from anyio import Path
from asyncpg import Connection, Pool, Record

from db.postgresql.core import query_db
from log_management import get_error_details
from synchronization import async_to_thread
from typestore.datatypes import AlpinePackageTuple, SessionHandler
from typestore.errors import DatabaseError, SQLTemplateError
from webtools.download import download_apkindex_files


logger = getLogger(__name__)


async def fetch_alpine_package_info(
        pool: Pool, package: str, project_directory: Path
) -> dict[str, str] | None:

    async def query_fn(connection: Connection, query: str) -> Record | None:
        return await connection.fetchrow(query, package)

    query_file = 'packages_get_package_info.sql'
    try:
        result = await query_db(pool, query_fn, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        return None
    return dict(result) if result is not None else None


async def create_alpine_packages_table(pool: Pool, project_directory: Path) -> None:

    async def query_fn(connection: Connection, query_qrg: str) -> str:
        return await connection.execute(query_qrg)

    query_file = 'packages_create_alpine_packages.sql'
    dbname, table_name = 'packages', 'alpine_packages'
    try:
        await query_db(pool, query_fn, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        logger.error('Table creation failed', extra={
            'database': dbname,
            'table_name':  table_name
        })
        raise
    logger.info('Successful creation of a table', extra={
        'database': dbname,
        'table_name': table_name
    })


async def update_alpine_packages_table(
        pool: Pool,
        project_directory: Path,
        session_manager: SessionHandler,
        semaphore: Semaphore,
) -> None:
    downloads_directory = project_directory / 'data' / 'downloaded'
    download_info = await download_apkindex_files(session_manager, downloads_directory, semaphore)
    files_to_process = [file for file in download_info if file is not None]
    if not files_to_process:
        logger.warning('Failed to download any APKINDEX file.')
        return
    packages_info_tuples = await _process_apkindex_files(files_to_process, downloads_directory, semaphore)

    async def query_fn(connection: Connection, query: str) -> None:
        return await connection.executemany(query, packages_info_tuples)

    query_file = 'packages_insert_alpine_packages.sql'
    dbname, table_name = 'packages', 'alpine_packages'
    try:
        await query_db(pool, query_fn, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        logger.error('Failed upsert', extra={
            'database': dbname,
            'table_name' : table_name
        })
    logger.info('Successful upsert', extra={
        'database': dbname,
        'table_name':  table_name
    })
    _remove_apkindex_files(files_to_process, downloads_directory)


async def _process_apkindex_files(
        files: list[str],
        downloads_directory: Path,
        semaphore: Semaphore
) -> list[AlpinePackageTuple]:
    tasks = [
        create_task(
            async_to_thread(
                semaphore, _process_apkindex, file, downloads_directory), name=str(uuid4())
        ) for file in files
    ]
    results = await gather(*tasks)
    packages_info = ChainMap(*(result for result in results if result is not None))
    return _correct_packages_info(packages_info)


def _process_apkindex(
        file_name: str,
        downloads_directory: Path
) -> dict[str, tuple[tuple[str, str, str, str], str]] | None:
    try:
        _extract_apkindex(file_name, downloads_directory)
    except (KeyError, OSError):
        return None
    try:
        return dict(_parse_apkindex(file_name, downloads_directory))
    except OSError as e:
        message = 'File parsing error'
        extra = get_error_details(e)
        extra['file_name'] = file_name
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        return None


def _parse_apkindex(file_name: str, downloads_directory: Path):
    package_details: tuple[str, str, str, str]
    apkindex_path = downloads_directory / f'{file_name}'
    with open(apkindex_path, 'r', encoding='utf-8') as f:
        package, version, description, homepage, license_info, srcname = ('',) * 6
        for line in f:
            line = line.strip()
            if not line:
                if package:
                    package_details = (srcname, homepage, description, license_info)
                    yield package, (package_details, version)
                    package, version, description, homepage, license_info, srcname = ('',) * 6
                continue
            if ':' in line:
                key, value = (part.strip() for part in line.split(':', 1))
                match key:
                    case 'P':
                        package = value
                    case 'V':
                        version = value
                    case 'T':
                        description = value
                    case 'U':
                        homepage = value
                    case 'L':
                        license_info = value
                    case 'o':
                        srcname = value
        if package:
            package_details = (srcname, homepage, description, license_info)
            yield package, (package_details, version)

def _extract_apkindex(file_name: str, downloads_directory: Path) -> None:
    apkindex_tar_path = downloads_directory / f'{file_name}.tar.gz'
    try:
        with (tarfile.open(apkindex_tar_path, 'r') as tar):
            try:
                member = 'APKINDEX'
                file_object = tar.extractfile(member)
            except KeyError as e:
                message = 'No APKINDEX file in the archive'
                extra = get_error_details(e)
                extra['archive_file'] = f'{file_name}.tar.gz'
                logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
                raise
            outfile_path = downloads_directory / f'{file_name}'
            with open(outfile_path, 'wb') as outfile:
                try:
                    outfile.write(file_object.read())
                except OSError as e:
                    message = 'Failed to save a file'
                    extra = get_error_details(e)
                    extra['archive_file'] = f'{file_name}.tar.gz'
                    logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
                    raise
    except OSError as e:
        message = 'Failed to open a file'
        extra = get_error_details(e)
        extra['file_name'] = f'{file_name}.tar.gz'
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise


def _correct_packages_info(
        packages_info: Mapping[str, tuple[tuple[str, str, str, str], str]]
) -> list[AlpinePackageTuple]:
    packages_info_corrected: dict[str, tuple[str, str, str, str]] = {}
    srcnames_description = {}
    for package in packages_info:
        package_details, version = packages_info[package]
        srcname, homepage, description, license_content = package_details
        srcname_parts = srcname.split('-')
        version_start = version.split('.', 1)[0]
        if re.search(r'^\d+$', version_start) is not None:
            srcname = '-'.join(part.replace(version_start, '') for part in srcname_parts)
        packages_info_corrected[package] = srcname, homepage, description, license_content
        package_description = f'{package} - {description}'
        if srcname in srcnames_description:
            srcnames_description[srcname] = f'{srcnames_description[srcname]}, {package_description}'
        else:
            srcnames_description[srcname] = package_description
    if '' in srcnames_description:
        srcnames_description.pop('')
    for package in packages_info_corrected:
        srcname, homepage, description, license_content = packages_info_corrected[package]
        packages_info_corrected[package] = (
            srcname, homepage, srcnames_description.get(srcname, description), license_content
        )
    return [
        AlpinePackageTuple(package, *packages_info_corrected[package]) for package in packages_info_corrected
    ]


def _remove_apkindex_files(files: Iterable, downloads_directory: Path) -> None:
    apkindex_paths = [downloads_directory / file for file in files]
    apkindex_tar_paths = [downloads_directory / f'{file}.tar.gz' for file in files]
    for path in apkindex_paths + apkindex_tar_paths:
        try:
            os.remove(path)
        except OSError as e:
            message = 'Failed to remove file.'
            extra = get_error_details(e)
            extra['file_name'] = path
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
