from asyncio import Lock, Semaphore
from logging import getLogger

from asyncpg import Connection, Pool, Record
from jinja2 import Environment

from linux_recognition.db.postgresql.core import query_db
from linux_recognition.typestore.errors import DatabaseError, SQLTemplateError


logger = getLogger(__name__)


class UDD:

    def __init__(
            self,
            raw_package_name,
            pool: Pool,
            environment: Environment,
            semaphore: Semaphore,
            udd_lock: Lock = Lock(),
            packages_table='all_packages',
            sources_table='all_sources',
    ):
        self._raw_package_name = raw_package_name
        self._pool = pool
        self._environment = environment
        self._semaphore = semaphore
        self._udd_lock = udd_lock
        self._packages_table = packages_table
        self._sources_table = sources_table
        self._source_package = ''
        self._homepage = ''

    def get_source_package(self) -> str:
        return self._source_package

    async def get_homepage(self) -> str:
        async with self._udd_lock:
            await self._fetch_source_package_name()
        if not self._source_package:
            return self._homepage

        async def query_fn(connection: Connection, query: str) -> Record:
            return await connection.fetchrow(query, self._source_package)

        query_file = 'udd_get_package_info.sql'
        try:
            async with self._udd_lock:
                record = await query_db(
                    self._pool,
                    self._environment,
                    query_fn,
                    query_file,
                    self._semaphore,
                    table_name=self._sources_table
                )
        except (DatabaseError, SQLTemplateError):
            logger.error('Database error', extra={
                'database': 'UDD',
                'table_name': self._sources_table,
            })
            return self._homepage
        if record is None:
            logger.debug('No such source package', extra={
                'database': 'UDD',
                'source_package': self._source_package
            })
            return self._homepage
        homepage = record['homepage']
        self._homepage = homepage if homepage is not None else ''
        if not self._homepage:
            logger.debug('Homepage for source package not found', extra={
                'database': 'UDD',
                'source_package': self._source_package
            })
        return self._homepage

    async def _fetch_source_package_name(self) -> None:

        async def query_fn(connection: Connection, query: str) -> Record:
            return await connection.fetchrow(query, self._raw_package_name)

        query_file = 'udd_get_source_package_name.sql'
        try:
            record = await query_db(
                self._pool,
                self._environment,
                query_fn,
                query_file,
                self._semaphore,
                table_name=self._packages_table
            )
        except (DatabaseError, SQLTemplateError):
            logger.error('UDD database error', extra={
                'database': 'UDD',
                'table_name': self._packages_table
            })
            return
        if record is None:
            logger.debug('No such binary package', extra={
                'database': 'UDD',
                'binary_package': self._raw_package_name
            })
            return
        source_package = record['source']
        self._source_package = source_package if source_package is not None else ''
        if not self._source_package:
            logger.debug('Source package not found', extra={
                'database': 'UDD',
                'binary_package': self._raw_package_name
            })
