import re
from collections.abc import Generator
from logging import DEBUG, getLogger
from xml.etree.ElementTree import Element
from zipfile import ZipFile

from anyio import Path
from asyncpg import Pool, Connection, Record
from defusedxml.ElementTree import iterparse

from db.postgresql.core import query_db
from log_management import get_error_details
from typestore.errors import DataDependencyError


logger = getLogger(__name__)


async def get_cpe_entities(
        pool: Pool,
        project_directory: Path,
        product_names: list[str]
) -> list[tuple[str, str, str]]:

    async def query_fn(connection: Connection, query: str) -> list[Record]:
        return await connection.fetch(query, tuple(product_names))

    query_file = 'packages_get_cpe_entities.sql'
    records = await query_db(pool, query_fn, query_file, project_directory)
    return [(record['publisher'], record['product'], record['version']) for record in records]


async def create_cpe_entities(pool: Pool, project_directory: Path) -> None:

    async def query_fn(connection: Connection, query: str) -> None:
        await connection.execute(query)

    query_file = 'packages_create_cpe_entities.sql'
    await query_db(pool, query_fn, query_file, project_directory)


async def create_product_index(pool: Pool, project_directory: Path) -> None:

    async def query_fn(connection: Connection, query: str) -> None:
        await connection.execute(query)

    query_file = 'packages_create_index_on_cpe_entities.sql'
    await query_db(pool, query_fn, query_file, project_directory)


async def populate_cpe_entities(pool: Pool, project_directory: Path, batch_size: int = 100000) -> None:
    # currently it is blocking function
    cpe_dictionary_file = 'cpe_dictionary.xml'
    cpe_dictionary_archive = f'{cpe_dictionary_file}.zip'
    downloads_directory = project_directory / 'data' / 'downloaded'
    try:
        _extract_cpe_dictionary(cpe_dictionary_archive, downloads_directory)
    except (KeyError, OSError) as e:
        raise DataDependencyError() from e
    cpe_dictionary_path = downloads_directory / cpe_dictionary_file
    entities_batch = []
    for entity in _search_for_cpe_entities(cpe_dictionary_path):
        entities_batch.append(entity)
        if len(entities_batch) <= batch_size:
            continue
        await _insert_entities_batch(pool, project_directory, entities_batch)
        entities_batch = []
    if entities_batch:
        await _insert_entities_batch(pool, project_directory, entities_batch)


async def _insert_entities_batch(
        pool: Pool,
        project_directory: Path,
        cpe_entities: list[tuple[str, str, str]]
) -> None:

    async def query_fn(connection: Connection, query: str) -> None:
        await connection.executemany(query, cpe_entities)

    query_file = 'packages_insert_cpe_entities.sql'
    await query_db(pool, query_fn, query_file, project_directory)


def _search_for_cpe_entities(file_path: Path) -> Generator[tuple[str, str, str], None, None]:
    namespace = 'http://scap.nist.gov/schema/cpe-extension/2.3'
    pattern = re.compile(r'cpe:2\.3:a:(?P<publisher>[^:]+):(?P<product>[^:]+):(?P<version>[^:]+)')
    for event, element in iterparse(file_path):
        element: Element = element
        element_name = element.get('name')
        if not element.tag == f'{{{namespace}}}cpe23-item' or element_name is None:
            continue
        match = pattern.match(element_name)
        if match is not None:
            yield match.group('publisher'), match.group('product'), match.group('version')
        element.clear()


def _extract_cpe_dictionary(archive_name: str, downloads_directory: Path) -> None:
    output_file = archive_name.rsplit('.', 1)[0]
    archive_path = downloads_directory / archive_name
    member = 'official-cpe-dictionary_v2.3.xml'
    try:
        with ZipFile(archive_path, 'r') as zip_file:
            try:
                member_info = zip_file.getinfo(member)
                member_info.filename = output_file
                zip_file.extract(member=member, path=downloads_directory)
            except KeyError as e:
                message = 'No cpe dictionary file in the archive'
                extra = get_error_details(e)
                extra['archive_name'] = archive_name
                extra['member_name'] = member
                logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
                raise
    except OSError as e:
        message = 'Failed to extract zip archive'
        extra = get_error_details(e)
        extra['archive_name'] = archive_name
        logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
        raise
