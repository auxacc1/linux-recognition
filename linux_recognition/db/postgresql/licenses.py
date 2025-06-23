import json
from asyncio import Semaphore
from logging import DEBUG, getLogger

from anyio import Path
from asyncpg import Connection, Pool, Record
from jinja2 import Environment

from linux_recognition.db.postgresql.core import query_db
from linux_recognition.log_management import get_error_details
from linux_recognition.typestore.datatypes import LicenseItem
from linux_recognition.typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


async def fetch_licenses(
        pool: Pool,
        environment: Environment,
        identifiers: list[str],
        semaphore: Semaphore
) -> list[LicenseItem]:

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        identifiers_tuples = [(identifier,) for identifier in identifiers]
        return await connection.fetchmany(query, identifiers_tuples)

    query_file = 'packages_get_license.sql'
    results = await query_db(pool, environment, query_fn, query_file, semaphore=semaphore)
    items = [LicenseItem(result['identifier'], result['name'], result['osi_approved']) for result in results]
    return items


async def fetch_identifiers(pool: Pool, environment: Environment, semaphore: Semaphore) -> list[str]:

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        return await connection.fetch(query)

    query_file = 'packages_get_license_identifier.sql'
    records = await query_db(pool, environment, query_fn, query_file, semaphore)
    return [record['identifier'] for record in records]


async def insert_licenses(
        pool: Pool,
        environment: Environment,
        semaphore: Semaphore,
        identifiers: list[str] | None = None,
        items: list[tuple[str, str, bool | None]] | None = None
) -> None:
    if identifiers is None:
        identifiers = []
    if items is None:
        items = []

    async def query_fn(connection: Connection, query: str) -> None:
        license_items = items[:]
        license_items.extend((identifier, identifier, None) for identifier in identifiers)
        return await connection.executemany(query, license_items)

    query_file = 'packages_insert_licenses.sql'
    try:
        await query_db(pool, environment, query_fn, query_file, semaphore)
    except (DatabaseError, SQLTemplateError):
        extra = {}
        if identifiers:
            extra['identifiers'] = identifiers
        if items:
            extra['items'] = items
        logger.error('Failed to insert license items', extra=extra)
        raise
    logger.info('Successful addition of a license item', extra={
        'database': 'packages',
        'table_name': 'licenses'
    })


async def create_licenses_table(pool: Pool, environment: Environment, semaphore: Semaphore) -> None:

    async def query_fn(connection: Connection, query: str) -> str:
        return await connection.execute(query)

    query_file = 'packages_create_licenses.sql'
    await query_db(pool,environment, query_fn, query_file, semaphore)
    logger.info('Successful creation of a table', extra={
        'database': 'packages',
        'table_name': 'licenses'
    })


async def populate_licenses_table(
        pool: Pool,
        environment: Environment,
        project_directory: Path,
        semaphore: Semaphore
) -> None:
    logger.info('Population of a table started', extra={
        'database': 'packages',
        'table_name': 'licenses'
    })
    spdx_licenses_file = 'licenses.json'
    try:
        licenses_data = await _load_licenses_data(project_directory, spdx_licenses_file)
    except Exception as e:
        message = 'Failed to load SPDX license data'
        extra = get_error_details(e)
        extra['filename'] = spdx_licenses_file
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise

    async def query_fn(connection: Connection, query: str) -> None:
        return await connection.executemany(query, licenses_data)

    query_file = 'packages_insert_licenses.sql'
    await query_db(pool, environment, query_fn, query_file, semaphore)
    logger.info('Successful population of a table', extra={
        'database': 'packages',
        'table_name': 'licenses'
    })


async def _load_licenses_data(project_directory: Path, licenses_file: str) -> list[tuple[str, str, bool]]:
    licenses_file = project_directory / 'data'/ 'downloaded' / licenses_file
    licenses_data = []
    async with await licenses_file.open('r') as afp:
        content = await afp.read()
        licenses: list[dict] = json.loads(content)['licenses']
        for item in licenses:
            licenses_data.append((item['name'], item['name'], item['isOsiApproved']))
            licenses_data.append((item['licenseId'], item['name'], item['isOsiApproved']))
    return licenses_data
