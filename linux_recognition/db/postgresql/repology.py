import re
import subprocess
from asyncio import Semaphore
from collections.abc import Callable, Iterable
from itertools import chain
from logging import DEBUG, getLogger
from platform import system
from os import environ
from shutil import which
from typing import Any

from anyio import Path
from asyncpg import Pool, Connection, Record
from jinja2 import Environment
from zstandard import ZstdDecompressor, ZstdError

from configuration import PostgresConfig
from db.postgresql.core import query_db
from log_management import get_error_details
from synchronization import async_to_thread
from typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


async def fetch_package_info(
        package: str,
        family: str | None,
        pool: Pool,
        environment: Environment,
        is_host_supported: Callable[[str], bool],
        semaphore: Semaphore
) -> dict[str, Any] | None:

    if family is None:

        async def query_fn(connection: Connection, query: str) -> list[Record]:
            return await connection.fetch(query, package)

        query_file = 'repology_get_info_for_package.sql'
        try:
            records = await query_db(pool, environment, query_fn, query_file, semaphore)
        except (DatabaseError, SQLTemplateError):
            return None
        if not records:
            return None
        record = _select_highest_priority_record(records, is_host_supported)
        return dict(record)

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        return await connection.fetch(query, family, package)

    query_file = 'repology_get_info_for_package_within_family.sql'
    try:
        records = await query_db(pool, environment, query_fn, query_file, semaphore)
    except DatabaseError:
        return None
    if not records:
        return None
    supported_homepage_record = next(
        (r for r in records if r['homepage'] and is_host_supported(r['homepage'])), None
    )
    if supported_homepage_record is not None:
        return dict(supported_homepage_record)
    family_constrained_record = next((r for r in records if r['homepage']), records[0])
    versions = [record['version'] for record in records if record['version']]
    if not versions:
        return dict(family_constrained_record)
    sql_patterns = _get_corresponding_sql_patterns(versions)
    projectname_seed = family_constrained_record['projectname_seed']

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        return await connection.fetch(query, projectname_seed, sql_patterns)

    query_file = 'repology_get_info_for_package_with_matching_version.sql'
    try:
        records = await query_db(pool, environment, query_fn, query_file, semaphore)
    except (DatabaseError, SQLTemplateError):
        return dict(family_constrained_record)
    record = _select_highest_priority_record(
        records, is_host_supported,
        family_constrained_record=family_constrained_record
    )
    return dict(record)


async def decompress_repology_database_dump(
        input_name: str,
        output_name: str,
        project_directory: Path,
        semaphore: Semaphore
) -> None:
    downloads_directory = project_directory / 'data' / 'downloaded'
    input_path =  downloads_directory / input_name
    output_path = downloads_directory / output_name
    try:
        await async_to_thread(semaphore, _zstd_decompress, input_path, output_path)
    except ZstdError as e:
        message = 'Failed to decompress repology database dump'
        extra = get_error_details(e)
        extra['file_name'] = input_name
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise


async def restore_repology_origin_database(
        pool: Pool,
        environment: Environment,
        project_directory: Path,
        dump_file: str,
        semaphore: Semaphore,
        postgres_config: PostgresConfig,
        psql_directory: Path | None = None
) -> None:
    await _restore_database(dump_file, postgres_config, project_directory, semaphore, psql_directory=psql_directory)
    schemas = ['public', 'repology']
    await _set_search_path(pool, environment, semaphore, postgres_config.dbname, schemas)


async def rebuild_repology_database(
        pool: Pool,
        environment: Environment,
        semaphore: Semaphore
) -> None:

    async def query_fn(connection: Connection, query: str) -> str:
        return await connection.execute(query)

    logger.info('Repology database rebuild started')
    execution_order = [
        'repology_create_seed_packages_no_urls.sql',
        'repology_create_links_ids.sql',
        'repology_create_seed_packages_urls.sql',
        'repology_drop_links_ids.sql',
        'repology_create_seed_packages_info.sql',
        'repology_drop_seed_packages_no_urls_and_seed_packages_urls.sql',
        'repology_create_packages_info.sql',
        'repology_create_indexes_on_packages_info.sql',
        'repology_drop_redundant.sql'
    ]
    for query_file in execution_order:
        try:
            command_status = await query_db(pool, environment, query_fn, query_file, semaphore)
        except (DatabaseError, SQLTemplateError):
            logger.critical('Database rebuild failed', extra={
                'database': 'repology',
                'query_file': query_file
            })
            raise
        logger.info(command_status, extra={'query_file': query_file})
    logger.info('Repology database rebuild completed')


def _select_highest_priority_record(
        records: list[Record],
        is_host_supported: Callable[[str], bool],
        family_constrained_record: Record = None
) -> Record:
    if family_constrained_record is not None:
        all_records = [family_constrained_record] + [r for r in records if r != family_constrained_record]
    else:
        all_records = records
    return next(
        chain(
            (r for r in records if r['homepage'] and is_host_supported(r['homepage'])
             and r['description'] and r['licenses']),
            (r for r in records if r['homepage'] and is_host_supported(r['homepage'])),
            (r for r in records if r['homepage'] and r['description'] and r['licenses']),
            (r for r in all_records if r['homepage']),
            (r for r in all_records if r['project_url']),
            (r for r in all_records if r['package_url']),
            all_records
        )
    )


def _get_corresponding_sql_patterns(versions: list[str]) -> list[str]:
    numeric_pattern = re.compile(r'^(\d+)\.(\d)')
    sql_version_patterns = []
    for version in versions:
        numeric_version_match = numeric_pattern.search(version)
        if numeric_version_match is not None:
            major = numeric_version_match.group(1)
            major_digits_count = len(major)
            minor_head = numeric_version_match.group(2)
            if minor_head is not None:
                pattern = f'{major[0]}{'_' * (major_digits_count - 1)}.{minor_head}%'
            else:
                pattern =  f'{major[0]}%'
            sql_version_patterns.append(pattern)
        else:
            sql_version_patterns.append(version)
    return list(set(sql_version_patterns))


def _zstd_decompress(input_path: Path, output_path: Path) -> None:
    dctx = ZstdDecompressor()
    with open(input_path, 'rb') as ifh, open(output_path, 'wb') as ofh:
        dctx.copy_stream(ifh, ofh, read_size=1048576, write_size=1048576)


async def _restore_database(
        dump_file: str,
        postgres_config: PostgresConfig,
        project_directory: Path,
        semaphore: Semaphore,
        psql_directory: Path | None = None
) -> None:
    try:
        await async_to_thread(
            semaphore,
            _execute_restore_command,
            dump_file,
            postgres_config,
            project_directory,
            psql_directory=psql_directory
        )
    except subprocess.SubprocessError as e:
        message = 'Repology database restore failed'
        extra = get_error_details(e)
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise
    logger.info('Repology database restore completed')


def _execute_restore_command(
        dump_file: str,
        postgres_config: PostgresConfig,
        project_directory: Path,
        psql_directory: Path | None = None
) -> None:
    psql_path = _resolve_psql_path(psql_directory)
    host = postgres_config.host
    port = str(postgres_config.port)
    user = postgres_config.user
    database = postgres_config.dbname
    dump_path = str(project_directory / 'data' / 'downloaded' / dump_file)
    env = environ.copy()
    env['PGPASSWORD'] = postgres_config.password
    arguments = [psql_path, '-h', host, '-p', port, '-U', user, '-d', database, '-f', dump_path]
    with subprocess.Popen(
            arguments,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
            encoding='utf-8'
    ) as process:
        for line in process.stdout:
            logger.info(line.strip, extra={'executable': 'psql'})
    return_code = process.poll()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, arguments)


def _resolve_psql_path(psql_directory: Path | None) -> str:
    psql_executable = 'psql.exe' if system() == 'Windows' else 'psql'
    psql_path = which(psql_executable)
    if psql_path is not None:
        return psql_path
    if psql_directory is None:
        raise FileNotFoundError('psql binary not found')
    psql_path = psql_directory / psql_executable
    if not psql_path.is_file():
        raise FileNotFoundError(f'psql binary not found at {psql_path}')
    return str(psql_path)


async def _set_search_path(
        pool: Pool,
        environment: Environment,
        semaphore: Semaphore,
        dbname: str,
        schemas: Iterable[str]
) -> None:

    async def query_fn(connection: Connection, query: str) -> str:
        return await connection.execute(query)

    query_file = 'repology_set_search_path.sql'
    await query_db(pool, environment, query_fn, query_file, semaphore, dbname=dbname, schemas=schemas)
