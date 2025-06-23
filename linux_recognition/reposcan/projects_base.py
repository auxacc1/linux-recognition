import re
from abc import ABC, abstractmethod
from asyncio import Lock, Semaphore
from collections.abc import Iterable, Mapping
from functools import reduce
from itertools import chain, count
from logging import getLogger
from typing import ClassVar, NamedTuple, Literal, Self
from xml.etree.ElementTree import Element

from defusedxml.ElementTree import fromstring

from linux_recognition.normalization import Fingerprint
from linux_recognition.reposcan.dateparse import extract_date_like, parse_date
from linux_recognition.reposcan.packages import LinuxPackage
from linux_recognition.typestore.datatypes import (
    Brand,
    ChangelogItem,
    Date,
    DevelopmentSuffix,
    GitHubTag,
    GitLabTag,
    GitTag,
    GitTagWithVersion,
    LicenseInfo,
    MeasuredRelease,
    RecognitionContext,
    Release,
    ReleaseInfo,
    ReleaseItem,
    SessionHandler,
    VersionInfo,
    VersionPattern
)
from linux_recognition.typestore.errors import ResponseError
from linux_recognition.webtools.content import fetch
from linux_recognition.webtools.response import JsonResponse, TextResponse


logger = getLogger(__name__)


class Project(ABC):

    is_source: ClassVar[bool]  = True
    _recognition_context: RecognitionContext
    _fingerprint: Fingerprint
    _session_manager: SessionHandler
    _url: str
    _membership_confirmed: bool
    _package_name: str
    _description: str
    _license_info: LicenseInfo
    _homepage: str
    _semaphore: Semaphore
    _version_separators: list[str]
    _version_info: VersionInfo | None

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        self._recognition_context = recognition_context
        self._fingerprint = fingerprint
        self._session_manager = recognition_context.session_handler
        self._url = url.rstrip('/')
        self._membership_confirmed = False
        if package_instance is not None:
            self._package_name = package_instance.get_name()
            self._description = package_instance.get_description()
            self._license_info = package_instance.get_license_info()
            self._log_inherited_repr()
        else:
            self._package_name = self._description = ''
            self._license_info = LicenseInfo([])
        self._homepage = ''
        self._semaphore = self._recognition_context.synchronization.semaphore
        self._version_separators = [r'\.', r'\-']
        self._version_info: VersionInfo | None = version_info
        if fingerprint.version_suffix is not None:
            self._version_with_suffix = f'{self._fingerprint.version} {fingerprint.version_suffix}'
        else:
            self._version_with_suffix = self._fingerprint.version

    def _log_inherited_repr(self) -> None:
        extra = {
            'package_name': self._package_name,
            'description': self._description,
            'license_info': self._license_info,
            'project_url': self._url
        }
        logger.debug('Inherited package data', extra=extra)

    def is_membership_confirmed(self) -> bool:
        return self._membership_confirmed

    @abstractmethod
    def get_homepage(self) -> str:
        pass

    @abstractmethod
    async def initialize(self) -> Self:
        pass

    @classmethod
    @abstractmethod
    def get_url_keys(cls) -> list[str]:
        pass

    @abstractmethod
    async def get_software(self) -> Brand:
        pass

    @abstractmethod
    async def get_publisher(self) -> Brand:
        pass

    @abstractmethod
    async def get_description(self) -> str:
        pass

    @abstractmethod
    async def get_license_info(self) -> LicenseInfo:
        pass

    @abstractmethod
    async def get_release(self, standalone: bool = False, **kwargs) -> Release:
        pass

    async def _fetch_json_response(self, url, **kwargs) -> JsonResponse:
        parameters = {
            'url': url,
            'session_manager': self._session_manager,
            'semaphore': self._semaphore
        }
        parameters.update(**kwargs)
        return await JsonResponse(**parameters).fetch()

    async def _fetch_text_response(self, url, **kwargs) -> TextResponse:
        parameters = {
            'url': url,
            'session_manager': self._session_manager,
            'semaphore': self._semaphore
        }
        parameters.update(**kwargs)
        return await TextResponse(**parameters).fetch()

    def _get_version_info(self) -> VersionInfo | None:
        version = self._fingerprint.version
        if not version:
            return None
        info = VersionInfo(version)
        info.is_date = self._fingerprint.version_is_date
        info.date_in_version = self._fingerprint.date_in_version
        separators_joined = r''.join(s for s in self._version_separators)
        separator = fr'\s?[{separators_joined}]\s?'
        no_space_separator = fr'[{''.join(s for s in self._version_separators if s != r'\s')}]'
        info.parts_pattern = fr'[^{separators_joined}{r'\s' if r'\s' not in self._version_separators else ''}]+'
        info.parts = re.findall(info.parts_pattern, version)
        info.parts_count = len(info.parts)
        last_significant_index = next(
            (j for j in range(info.parts_count - 1, -1, -1) if not set(info.parts[j]).issubset({'0'})),
            info.parts_count - 1
        )
        significant_parts = info.parts[:last_significant_index + 1]
        info.is_digital = all(re.search(r'\D', part) is None for part in significant_parts)
        info.suffix = self._fingerprint.version_suffix
        significant = significant_parts if last_significant_index > 0 else info.parts[:2]
        general = fr'(?:^|\s)v?{separator.join([re.escape(p) for p in significant])}(?![^\D0])'
        strict_general = fr'(?:^|\s)v?{no_space_separator.join([re.escape(p) for p in significant])}(?![^\D0])'
        if info.suffix is not None:
            info.in_development = True
            exact = fr'{general}(?:{separator}0+)*(?:[\W_\s]+)?{info.suffix}\b'
            strict_exact = fr'{strict_general}(?:{no_space_separator}0+)*(?:[\W_\s]+)?{info.suffix}\b'
        else:
            suffix_pattern = DevelopmentSuffix.pattern.pattern
            exact = fr'{general}(?:{separator}0+)*(?=$|[\W_\s]+(?!\d|{suffix_pattern}))'
            strict_exact = fr'{strict_general}(?:{no_space_separator}0+)*(?=$|[\W_\s]+(?!\d|{suffix_pattern}))'
        info.pattern = VersionPattern(
            re.compile(general, re.IGNORECASE),
            re.compile(exact, re.IGNORECASE),
            separator
        )
        info.pattern_strict = VersionPattern(
            re.compile(strict_general, re.IGNORECASE),
            re.compile(strict_exact, re.IGNORECASE),
            no_space_separator
        )
        return info

    def _fetch_from_changelog(self, changelog: str) -> Release:
        version = self._version_with_suffix
        iso_date = ''
        if not changelog or self._version_info is None:
            return Release(version, iso_date)
        changelog_lines = changelog.splitlines()
        version_pattern = self._version_info.pattern_strict
        general_pattern = version_pattern.general
        exact_match: ChangelogItem | None = None
        partial_matches = []
        for index, line in enumerate(changelog_lines):
            version_match = general_pattern.search(line)
            if version_match is not None:
                exact_pattern = version_pattern.exact
                version_exact_match = exact_pattern.search(line)
                if version_exact_match is not None:
                    exact_match = ChangelogItem(match=version_exact_match, line_index=index)
                    break
                tail = Project._get_version_tail(line[version_match.end():])
                partial_matches.append(ChangelogItem(match=version_match, line_index=index, tail=tail))
        logger.debug('Changelog matches', extra={
            'partial_matches': partial_matches,
            'exact_match': exact_match
        })
        if exact_match is not None:
            best_match = exact_match
        else:
            if not partial_matches:
                return Release(version)
            best_match = self._get_best_match(partial_matches)
        match = best_match.match
        line_index = best_match.line_index
        line = changelog_lines[line_index]
        search_area = line[:match.start()] + line[match.end():]
        date_patterns = self._recognition_context.date_patterns
        date_like_parse = extract_date_like(search_area, patterns=date_patterns)
        if date_like_parse is not None:
            date = parse_date(date_like=date_like_parse, patterns=date_patterns)
            if date is not None:
                iso_date = date.iso_format()
        else:
            date = parse_date(search_area, patterns=date_patterns, no_year=True)
            if date is not None:
                year_pattern = re.compile(r'\b(?:19|20)\d{2}\b')
                for l in changelog_lines[line_index + 1:]:
                    year_match = year_pattern.search(l)
                    if year_match is not None:
                        date.year = int(year_match.group())
                        date = date.check_day_in_month()
                        if date is not None:
                            iso_date = date.iso_format()
        return Release(version, iso_date)

    async def _fetch_release_from_rss(self, url: str) -> Release:
        matching_items: list[ReleaseInfo] = []
        matched_item = await self._search_rss_feed(url, matching_items)
        if matched_item is None:
            if not matching_items:
                return Release(self._version_with_suffix)
            matched_item = self._get_best_match(matching_items)
        date_patterns = self._recognition_context.date_patterns
        date = parse_date(matched_item.date_info, patterns=date_patterns)
        if date is None:
            return Release(self._version_with_suffix)
        return Release(self._version_with_suffix, date.iso_format())

    async def _search_rss_feed(self, rss_url: str, matching_items: list[ReleaseInfo]) -> ReleaseInfo | None:
        try:
            response = await self._fetch_text_response(rss_url)
        except ResponseError:
            return None
        feeds_as_text = response.get_content()
        root = fromstring(feeds_as_text)
        channel = root.find('channel')
        iterator = channel.iterfind('./item') if channel is not None else root.iterfind('.//item')
        for element in iterator:
            scanning_result = self._scan_element(element, matching_items)
            if scanning_result is not None:
                return scanning_result
        return None

    def _scan_element(self, element: Element, partial_matches: list[ReleaseInfo]) -> ReleaseInfo | None:
        general_pattern = self._version_info.pattern.general
        exact_pattern = self._version_info.pattern.exact
        scanned_tag = element.find('title')
        if scanned_tag is None:
            return None
        release_name = scanned_tag.text
        match = general_pattern.search(release_name)
        if match is not None:
            logger.debug('RSS feed scan', extra={'matched_item': release_name})
            date_published = element.find('pubDate')
            if date_published is None:
                return None
            date_info = date_published.text.strip()
            exact_match = exact_pattern.search(release_name)
            if exact_match is not None:
                return ReleaseInfo(match=exact_match, date_info=date_info)
            tail = self._get_version_tail(release_name[match.end():])
            release_info = ReleaseInfo(match=match, date_info=date_info, tail=tail)
            partial_matches.append(release_info)
            return None
        return None

    def _get_best_match[ReleaseItemType: ReleaseItem](
            self,
            matching_items: list[ReleaseItemType]
    ) -> ReleaseItemType:
        suffix_base = DevelopmentSuffix.base
        stable = [item for item in matching_items if re.search(fr'[ab]|{suffix_base}', item.tail) is None]
        stable_non_digital = [item for item in stable if re.search(r'\d+', item.tail) is None]
        digital_measured = [
            MeasuredRelease(item, self._digital_tail_value(item.tail))
            for item in stable if item not in stable_non_digital
        ]
        digital_with_value = [i for i in digital_measured if i.value is not None]
        if digital_with_value:

            def minimizer(current: MeasuredRelease, item: MeasuredRelease) -> MeasuredRelease:
                return item if item.value < current.value else current

            best_match = reduce(minimizer, digital_with_value).item
        else:
            best_match = next(chain(stable, matching_items))
        return best_match

    @staticmethod
    def _get_version_tail(extracted: str) -> str:
        suffix_match = re.search(fr'[\W_\s]+{DevelopmentSuffix.pattern}', extracted)
        if suffix_match is not None:
            return extracted[:suffix_match.end()]
        bound_match = re.search(r'\s', extracted)
        return extracted[:bound_match.start()] if bound_match is not None else extracted

    def _digital_tail_value(self, tail: str) -> float | None:
        separator = self._version_info.pattern_strict.separator
        number_pattern = fr'(?<={separator})\d+(?:{separator}\d+)*'
        number_match = re.search(number_pattern, tail)
        if number_match is None:
            return None
        matched = '.'.join(re.split(separator, number_match.group()))
        return float(matched)

    @staticmethod
    def _resolve_names(names: Iterable, redundant: Iterable):
        return list(
            {n.lower(): n for n in names if n.lower() not in redundant}.values()
        )


class GitProject(Project, ABC):

    _mode: Literal['GitHub', 'GitLab']
    _project_url: str
    _lock: Lock

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info=version_info)

    async def _fetch_tag_with_version[Tag: GitTag](self) -> GitTagWithVersion[Tag] | None:
        if self._version_info is None:
            return None
        if self._mode == 'GitHub':
            tags_url = f'{self._project_url}/tags'
        else:
            tags_url = f'{self._project_url}/repository/tags'
        for page_number in count(1):
            params = {
                'per_page': 100,
                'page': page_number
            }
            async with self._lock:
                try:
                    response = await self._fetch_json_response(tags_url, params=params)
                except ResponseError:
                    return None
            tags = response.get_content()
            if not isinstance(tags, list) or not tags:
                return None
            if (tag_with_version := self._match_tag_with_version(tags)) is not None:
                return tag_with_version
        return None

    def _match_tag_with_version[TV: GitTagWithVersion](self, tags: list[Mapping]) -> TV | None:
        if self._version_info.is_date:
            return self._match_date_tag_with_version(tags)
        version = self._version_info.name
        general_pattern = self._version_info.pattern.general
        exact_pattern = self._version_info.pattern.exact
        matching_tags: list[GitTag] = []
        for tag in tags:
            name = fetch(tag, 'name', output_type=str).strip()
            match = general_pattern.search(name)
            if match is None:
                continue
            if self._mode == 'GitHub':
                url = fetch(tag, 'commit', 'url', output_type=str).strip()
                matched_tag = GitHubTag(match=match, name=name, url=url)
            else:
                date = fetch(tag, 'commit', 'committed_date', output_type=str).strip()
                matched_tag = GitLabTag(match=match, name=name, date=date)
            if exact_pattern.search(name) is not None:
                return GitTagWithVersion(matched_tag, version)
            matched_tag.tail = self._get_version_tail(name[match.end():])
            matching_tags.append(matched_tag)
        if not matching_tags:
            return None
        matched_tag = self._get_best_match(matching_tags)
        return GitTagWithVersion(matched_tag, version) if matched_tag is not None else None

    def _match_date_tag_with_version[TV: GitTagWithVersion](self, tags: list[Mapping]) -> TV | None:

        class DateTagInfo(NamedTuple):
            tag: GitTag
            fully_matching: bool
            suffix_consistent: bool

        version_info = self._version_info
        date_in_version: Date = version_info.date_in_version
        year = date_in_version.year
        month = date_in_version.month
        potential_matching_tags = []
        for tag in tags:
            name = fetch(tag, 'name', output_type=str).strip()
            if str(year) in name and str(month) in name:
                potential_matching_tags.append((tag, name))
        matching_info: list[DateTagInfo] = []
        for tag, name in potential_matching_tags:
            date_patterns = self._recognition_context.date_patterns
            date_like = extract_date_like(name, patterns=date_patterns)
            if date_like is None:
                continue
            date_from_tag = parse_date(date_like=date_like)
            if date_from_tag is None or not date_from_tag.is_close_to(date_in_version):
                continue
            match = date_like.match
            tail = Project._get_version_tail(name[match.end():])
            if self._mode == 'GitHub':
                url = fetch(tag, 'commit', 'url', output_type=str).strip()
                matched_tag = GitHubTag(match=match, tail=tail, name=name, url=url)
            else:
                date = fetch(tag, 'commit', 'committed_date', output_type=str).strip()
                matched_tag = GitLabTag(match=match, name=name, date=date)
            date_tag_info = DateTagInfo(
                tag=matched_tag,
                fully_matching=date_from_tag == date_in_version,
                suffix_consistent=GitProject._is_version_suffix_consistent(version_info, tail)
            )
            if date_tag_info.fully_matching and date_tag_info.suffix_consistent:
                return GitTagWithVersion(matched_tag, version_info.name)
            matching_info.append(date_tag_info)
        best_match: GitTag | None = next(
            chain(
                (info.tag for info in matching_info if info.fully_matching),
                (info.tag for info in matching_info if info.suffix_consistent),
                (info.tag for info in matching_info)
            ),
            None
        )
        if best_match is not None:
            version = best_match.match.group()
            return GitTagWithVersion(best_match, version)
        return None

    @staticmethod
    def _is_version_suffix_consistent(version_info: VersionInfo, tail: str):
        if version_info.in_development:
            if version_info.suffix in tail:
                return True
            else:
                return False
        else:
            suffix_pattern = re.compile(DevelopmentSuffix.base)
            return True if suffix_pattern.search(tail) is None else False
