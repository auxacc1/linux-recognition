from __future__ import annotations

import re

from asyncio import Lock, Semaphore
from calendar import monthrange
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import NamedTuple, Protocol, TypedDict, runtime_checkable

from aiohttp import ClientSession
from anyio import Path
from asyncpg import Pool


@runtime_checkable
class Package(Protocol):

    async def initialize(self) -> Package: ...

    def get_name(self) -> str: ...

    def get_package_url(self) -> str: ...

    def get_vendor(self) -> str: ...

    def get_homepage(self) -> str: ...

    def get_description(self) -> str: ...

    def get_license_info(self) -> LicenseInfo: ...


@dataclass(frozen=True)
class PackageTools:
    classes: list[Package]
    family: str

    @property
    def dict_repr(self):
        return {
            'classes': [cls.__name__ for cls in self.classes],
            'family': self.family
        }


class AlpinePackageTuple(NamedTuple):
    package: str
    srcname: str
    homepage: str
    description: str
    license_content: str


@dataclass
class Date:
    year: int | None
    month: int
    day: int

    def check_day_in_month(self) -> Date | None:
        if self.year is None:
            return self
        if self.day > monthrange(self.year, self.month)[1]:
            return None
        return self

    def iso_format(self) -> str:
        if self.year is None:
            return f'{self.month:02d}-{self.day:02d}'
        return f'{self.year}-{self.month:02d}-{self.day:02d}'

    def is_close_to(self, date_obj: Date) -> bool:
        if date_obj is None or not all((getattr(self, p) == getattr(date_obj, p) for p in ['year', 'month'])):
            return False
        return True


class ParsedDateLike(NamedTuple):
    year: int | None
    non_year_0: int
    non_year_1: int | None = None


class DateLikeParse(NamedTuple):
    parsed: ParsedDateLike
    ordered: bool
    match: re.Match
    mo_year: bool = False


class HTMLParse(NamedTuple):
    parsed: str = ''
    raw: str = ''
    url: str = ''


class Release(NamedTuple):
    version: str = ''
    date: str = ''


class DevelopmentSuffix:
    base: str = r'(?:rc|α(?:lpha)?|β(?:eta)?|alpha|beta)'
    rest: str = r'_?\d+'
    pattern: re.Pattern = re.compile(fr'\b\d*({base}(?:{rest})?)\b')


class VersionPattern(NamedTuple):
    general: re.Pattern
    exact: re.Pattern
    separator: str = r''


@dataclass
class VersionInfo:
    name: str
    is_digital: bool = None
    is_date: bool | None = None
    date_in_version: Date | None = None
    parts: list[str] | None = None
    parts_count: int | None = None
    parts_pattern: str = r'[^\s.-]+'
    pattern: VersionPattern | None = None
    pattern_strict: VersionPattern | None = None
    in_development: bool = False
    suffix: str | None = None


@dataclass
class PerlRelease:
    version: str = ''
    date: str = ''
    url: str = ''
    changes_url: str = ''


@dataclass
class PerlModule:
    name: str = ''
    description: str = ''
    metacpan_url: str = ''
    is_main: bool = False


@dataclass
class PerlDistribution:
    name: str = ''
    abstract: str = ''
    description: str = ''
    metacpan_html_url: str = ''
    provides:  list = field(default_factory=list)
    obsolete: bool = False
    name_of_current: str = ''


@dataclass
class PerlAuthor:
    name: str = ''
    abbr: str = ''


@dataclass
class PerlDistributionInfo:
    distribution: PerlDistribution | None = None
    modules: list[PerlModule] | None = None
    author: PerlAuthor | None = None
    licenses: list[str] = field(default_factory=list)
    homepage: str = ''
    latest_release: PerlRelease | None = None
    required_release: PerlRelease | None = None


@dataclass
class ReleaseItem:
    match: re.Match | None = None
    tail: str = ''


@dataclass
class GitTag(ReleaseItem):
    name: str = ''


@dataclass
class GitHubTag(GitTag):
    name: str = ''
    url: str = ''


@dataclass
class GitLabTag(GitTag):
    name: str = ''
    date: str = ''


@dataclass
class ReleaseInfo(ReleaseItem):
    date_info: str = ''


@dataclass
class ChangelogItem(ReleaseItem):
    line_index: int = -1


class GitTagWithVersion[Tag: GitTag](NamedTuple):
    tag: Tag
    version: str


class MeasuredRelease(NamedTuple):
    item: ReleaseItem
    value: float | None


@dataclass(frozen=True)
class Brand:
    name: str = ''
    alternative_names: list[str] = field(default_factory=list)


class LicenseInfo(NamedTuple):
    content: list[str]
    is_raw_text: bool = False


class LicenseItem(NamedTuple):
    identifier: str
    name: str
    osi_approved: bool


class LicenseIdentifiers(NamedTuple):
    recognized: list[str]
    unrecognized: list[str]


@dataclass(frozen=True)
class Fingerprint:
    software: str
    publisher: str
    version: str
    _version_uncut: str = field(default='', repr=False, compare=False)
    _date_in_version: Date | None = field(default=None, repr=False, compare=False)
    _version_is_date: bool = field(default=False, repr=False, compare=False)
    _version_suffix: str | None = field(default=None, repr=False, compare=False)

    @property
    def version_uncut(self) -> str:
        return self._version_uncut

    @property
    def date_in_version(self) -> Date:
        return self._date_in_version

    @property
    def version_is_date(self) -> bool:
        return self._version_is_date

    @property
    def version_suffix(self) -> str | None:
        return self._version_suffix

    def db_repr(self) -> tuple[str | None, str | None, str | None]:
        return self.software or None, self.publisher or None, self.version or None

    @staticmethod
    def from_triple(fp_triple) -> Fingerprint:
        return Fingerprint(fp_triple[0] or '', fp_triple[1] or '', fp_triple[2] or '')


class FingerprintDict(TypedDict):
    software: str
    publisher: str
    version: str


class VersionNormalizationPatterns(NamedTuple):
    suffix: re.Pattern = DevelopmentSuffix.pattern
    date: re.Pattern = re.compile(r'(?P<y>(?:19|20)\d{2})\.?(?P<m>\d{1,2})\.?(?P<d>\d{1,2})?')
    separator: re.Pattern = re.compile(r'([\W_]+)')


class SourceDbPools(NamedTuple):
    repology: Pool
    packages: Pool
    udd: Pool


class DbPools(NamedTuple):
    recognized: Pool
    source: SourceDbPools


class DatePatternsComplete(NamedTuple):
    digital: re.Pattern
    word_month: re.Pattern
    no_separator: re.Pattern


class DatePatternsNoYear(NamedTuple):
    digital: re.Pattern
    word_month: re.Pattern


class DatePatterns(NamedTuple):
    complete: DatePatternsComplete
    no_year: DatePatternsNoYear


class LibraryPatterns:
    python: re.Pattern = re.compile(
        r'python(?P<version>[23](?:\d{1,2})?)[\s-]+(?P<package>[^\s-]\S*)', re.IGNORECASE
    )
    ruby: re.Pattern = re.compile(r'ruby(?:gem)?[\s-]+([^\s-]\S*)', re.IGNORECASE)
    perl: re.Pattern = re.compile(
        r'perl-\s*(?P<fedora>[^\s-].+)$|^lib(?P<debian>[^\s-].+)-\s*perl$', re. IGNORECASE
    )


@dataclass(frozen=True)
class SynchronizationPrimitives:
    semaphore: Semaphore
    github_lock: Lock
    gitlab_lock: Lock
    udd_lock: Lock
    google_lock: Lock
    logging_lock: Lock

    @classmethod
    def create(cls) -> SynchronizationPrimitives:
        semaphore = Semaphore(50)
        github_lock = Lock()
        gitlab_lock = Lock()
        udd_lock = Lock()
        google_lock = Lock()
        logging_lock = Lock()
        return cls(
            semaphore=semaphore,
            github_lock=github_lock,
            gitlab_lock=gitlab_lock,
            udd_lock=udd_lock,
            google_lock=google_lock,
            logging_lock=logging_lock
        )


@runtime_checkable
class SessionHandler(Protocol):

    def get_session(self, session_name: str = 'common') -> ClientSession: ...

    async def close_sessions(self) -> None: ...


@runtime_checkable
class LlmInteraction(Protocol):

    async def generate_formal_definition(self, text: str, software: str) -> str: ...

    async def extract_licenses(self, text: str, software: str) -> list[str]: ...


@runtime_checkable
class LicenseResolver(Protocol):

    async def create_vectorstore(self) -> None: ...

    async def resolve(self, identifiers: list[str]) -> list[str]: ...


class RecognitionContext(NamedTuple):
    project_directory: Path
    session_handler: SessionHandler
    recognized_db_pool: Pool
    source_db_pools: SourceDbPools
    llm_interaction: LlmInteraction
    license_resolver: LicenseResolver
    is_host_supported: Callable[[str], bool]
    date_patterns: DatePatterns
    library_patterns: LibraryPatterns
    synchronization: SynchronizationPrimitives


class RecognitionResult(NamedTuple):
    fingerprint: Fingerprint
    software: Brand
    publisher: Brand
    description: str
    licenses: list[str]
    homepage: str
    version: str
    release_date: str
    cpe_string: str
    unspsc: str
