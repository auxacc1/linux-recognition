import re
from collections.abc import Callable
from itertools import chain
from logging import getLogger
from typing import Any

from anyio import Path
from asyncpg import Pool, Connection, Record

from db.postgresql.core import query_db
from typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


async def fetch_package_info(
        package: str,
        family: str | None,
        pool: Pool,
        is_host_supported: Callable[[str], bool],
        project_directory: Path
) -> dict[str, Any] | None:

    if family is None:

        async def query_fn(connection: Connection, query: str) -> list[Record]:
            return await connection.fetch(query, package)

        query_file = 'repology_get_info_for_package.sql'
        try:
            records = await query_db(pool, query_fn, query_file, project_directory)
        except DatabaseError:
            return None
        if not records:
            return None
        record = _select_highest_priority_record(records, is_host_supported)
        return dict(record)

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        return await connection.fetch(query, family, package)

    query_file = 'repology_get_info_for_package_within_family.sql'
    try:
        records = await query_db(pool, query_fn, query_file, project_directory)
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
        records = await query_db(pool, query_fn, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        return dict(family_constrained_record)
    record = _select_highest_priority_record(
        records, is_host_supported,
        family_constrained_record=family_constrained_record
    )
    return dict(record)


async def rebuild_repology_database(pool: Pool, project_directory: Path) -> None:

    async def query_fn(connection: Connection, query: str) -> str:
        return await connection.execute(query)

    execution_order = [
        'repology_create_src_packages_no_urls.sql',
        'repology_create_link_ids.sql',
        'repology_create_package_urls.sql',
        'repology_drop_link_ids.sql',
        'repology_create_packages_info.sql',
        'repology_drop_src_packages_no_links_and_packages_urls.sql',
        'repology_create_packages_info.sql',
        'repology_create_indexes_on_packages_info.sql',
        'repology_drop_redundant_tables.sql'
    ]
    for query_file in execution_order:
        try:
            await query_db(pool, query_fn, query_file, project_directory)
        except (DatabaseError, SQLTemplateError):
            logger.critical('Database rebuild failed', extra={
                'database': 'repology',
                'query_file': query_file
            })
            raise


async def fetch_table_names(pool: Pool, project_directory: Path) -> list[str]:

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        return await connection.fetch(query)

    query_file = 'repology_get_table_names.sql'
    try:
        records = await query_db(pool, query_fn, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        logger.critical('Failed to fetch repology database table names', extra={
            'database': 'repology',
            'query_file': query_file
        })
        raise
    return [record['tablename'] for record in records]



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
