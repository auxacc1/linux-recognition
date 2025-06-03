from logging import getLogger

from anyio import Path
from asyncpg import Connection, Pool

from db.postgresql.core import query_db
from normalization import Fingerprint
from typestore.datatypes import RecognitionResult
from typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


async def filter_recognized_fingerprints(
        pool: Pool,
        project_directory: Path,
        fingerprints: list[Fingerprint]
) -> list[Fingerprint]:

    async def query_fn(connection: Connection, query: str) -> list[Fingerprint]:
        args = [fingerprint.db_repr() for fingerprint in fingerprints]
        recognized_fingerprint_triples = await connection.fetchmany(query, args)
        recognized_fingerprints = [
            Fingerprint.from_triple(fp_triple) for fp_triple in recognized_fingerprint_triples
        ]
        return [fp for fp in fingerprints if fp not in recognized_fingerprints]

    query_file = 'recognized_get_fingerprint.sql'
    try:
        return await query_db(pool, query_fn, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        message = 'Failed to filter out recognized fingerprints'
        extra = {
            'database': 'recognized',
            'table_name': 'software_info'
        }
        logger.error(message, extra=extra)
        raise


async def update_recognized_table(
        pool: Pool,
        project_directory: Path,
        recognition_results: list[RecognitionResult]
) -> None:
    if not recognition_results:
        return

    async def _update_recognized_table(connection: Connection, query: str) -> None:
        result_args = [
            (
                *result.fingerprint.db_repr(),
                result.software.name,
                result.software.alternative_names,
                result.publisher.name,
                result.publisher.alternative_names,
                result.description,
                result.licenses,
                result.homepage,
                result.version,
                result.release_date,
                result.cpe_string,
                result.unspsc
            )
            for result in recognition_results
        ]
        return await connection.executemany(query, result_args)

    query_file = 'recognized_insert_software_info.sql'
    fingerprints = [repr(result.fingerprint) for result in recognition_results]
    dbname, table_name = 'recognized', 'software_info'
    try:
        await query_db(pool, _update_recognized_table, query_file, project_directory)
    except (DatabaseError, SQLTemplateError):
        message ='Insert error'
        logger.error(message, extra={
            'database': dbname,
            'table_name': table_name
        })
        raise
    logger.info('Data for fingerprints inserted successfully', extra={
        'database': dbname,
        'table_name': table_name,
        'fingerprints': fingerprints
    })


async def create_output_table(pool: Pool, project_directory: Path) -> None:

    async def query_fn(connection: Connection, query: str) -> str:
        return await connection.execute(query)

    query_file = 'recognized_create_software_info.sql'
    dbname, table_name = 'recognized', 'software_info'
    try:
        await query_db(
            pool,
            query_fn,
            query_file,
            project_directory
        )
    except (DatabaseError, SQLTemplateError):
        message = 'Table creation failed'
        logger.error(message, extra={
            'database': dbname,
            'table_name': table_name
        })
        raise
    logger.info('Table created successfully', extra={
        'database': dbname,
        'table_name': table_name
    })
