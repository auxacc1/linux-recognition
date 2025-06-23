import re
import sys
from asyncio import Lock
from base64 import b64decode
from binascii import Error as BinasciiError
from collections.abc import Mapping
from dataclasses import astuple
from functools import reduce
from inspect import getmembers, isclass
from logging import DEBUG, getLogger
from os import getenv
from typing import Any, ClassVar, Self
from urllib.parse import quote, urljoin, urlparse, ParseResult
from xml.etree.ElementTree import Element

from linux_recognition.log_management import get_error_details
from linux_recognition.reposcan.packages import FedoraPackage, LinuxPackage
from linux_recognition.reposcan.projects_base import GitProject, Project
from linux_recognition.synchronization import async_to_thread
from linux_recognition.typestore.datatypes import (
    Brand,
    Fingerprint,
    GitHubTag,
    GitTagWithVersion,
    LicenseInfo,
    PerlAuthor,
    PerlDistribution,
    PerlDistributionInfo,
    PerlModule,
    PerlRelease,
    Release,
    ReleaseInfo,
    RecognitionContext,
    VersionInfo
)
from linux_recognition.typestore.errors import ResponseError
from linux_recognition.webtools.content import fetch
from linux_recognition.webtools.response import JsonResponse, TextResponse


logger = getLogger(__name__)


class GitHubProject(GitProject):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        self._mode = 'GitHub'
        self._lock: Lock = self._recognition_context.synchronization.github_lock
        self._api_base_url: str = 'https://api.github.com/'
        self._project_url: str = self._get_api_url_for_project()
        self._version_separators: list[str] = [r'\.', r'\-', r'_']
        self._project_info: Mapping = {}
        if version_info is None:
            self._version_info: VersionInfo | None = self._get_version_info()

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['github.com']

    async def initialize(self) -> Self:
        await self._load_project_info()
        return self

    async def get_software(self) -> Brand:
        if not self._project_info:
            return Brand(self._package_name)
        project_name = fetch(self._project_info, 'name', output_type=str).strip()
        name_from_url = self._url.rsplit('/', 1)[-1]
        name = project_name or name_from_url or self._package_name
        other_names = {name_from_url, self._package_name}
        redundant = ['', name.lower()]
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)


    async def get_publisher(self) -> Brand:
        url_parts = self._url.rsplit('/', 2)
        author_from_url = url_parts[1] if len(url_parts) == 3 else ''
        author_url = fetch(self._project_info, 'owner', 'url', output_type=str).strip()
        if not author_url:
            return Brand(author_from_url)
        async with self._lock:
            try:
                response = await self._fetch_json_response(author_url)
            except ResponseError:
                return Brand(author_from_url)
        author_info: Mapping[str, Any] = response.get_content()
        author_name = fetch(author_info, 'name', output_type=str).strip()
        author_login = fetch(author_info, 'login', output_type=str).strip()
        name = author_name or author_login or author_from_url
        other_names = {author_login, author_from_url}
        redundant = ['', name.lower()]
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)

    def get_homepage(self) -> str:
        homepage = fetch(self._project_info, 'homepage', output_type=str).strip()
        self._homepage = homepage if homepage else self._url
        return self._homepage

    async def get_description(self) -> str:
        minimal_length = 10
        description = fetch(self._project_info, 'description', output_type=str).strip()
        if len(description) > minimal_length:
            self._description = description
            return self._description
        elif len(self._description) >= minimal_length:
            return self._description
        readme_url = f'{self._project_url}/readme'
        async with self._lock:
            try:
                response = await self._fetch_json_response(readme_url)
            except ResponseError:
                if description:
                    self._description = description
                return self._description
        readme_info: Mapping[str, Any] = response.get_content()
        description_encoded = fetch(readme_info, 'content', output_type=str)
        try:
            readme_bytes = b64decode(description_encoded)
        except BinasciiError as e:
            message = 'GitHub project readme decoding error'
            extra = get_error_details(e)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            return self._description
        readme = str(readme_bytes).strip()[:4096]
        combined_description = '\n'.join(component for component in [description, readme] if component)
        if combined_description:
            self._description = combined_description
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        license_metadata = fetch(self._project_info, 'license')
        if license_metadata is None:
            return self._license_info
        license_name = fetch(license_metadata, 'name', output_type=str).strip()
        if license_name.lower() not in ['other', '']:
            self._license_info = LicenseInfo([license_name])
            return self._license_info
        license_url = f'{self._project_url}/license'
        async with self._lock:
            try:
                response = await self._fetch_json_response(license_url)
            except ResponseError:
                return self._license_info
        license_info: Mapping[str, Any] = response.get_content()
        license_name = fetch(license_info, 'license', 'name', output_type=str).strip()
        if license_name.lower() not in ['other', '']:
            self._license_info = LicenseInfo([license_name])
            return self._license_info
        license_encoded = fetch(license_info, 'content', output_type=str)
        try:
            license_bytes = b64decode(license_encoded)
        except BinasciiError as e:
            message = 'GitHub project license decoding error'
            extra = get_error_details(e)
            logger.error(message, exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            return self._license_info
        license_content = str(license_bytes).strip()
        self._license_info = LicenseInfo([license_content.strip()[:4096]], is_raw_text=True)
        return self._license_info

    async def get_release(self, standalone: bool = False, changelog_uri: str | None = None) -> Release:
        if self._version_info is None:
            return Release()
        if changelog_uri is not None:
            release = await self._get_release_from_changelog(changelog_uri)
            if isinstance(release, Release):
                return release
        version_is_date = self._version_info.is_date
        date_in_version = self._version_info.date_in_version
        release_date_from_version = date_in_version.iso_format() if version_is_date else ''
        if (tag_with_version := await self._fetch_tag_with_version()) is None:
            return Release(self._version_with_suffix, release_date_from_version)
        return await self._get_release_for_tag(tag_with_version)

    async def _get_release_for_tag(self, tag_with_version: GitTagWithVersion) -> Release:
        tag: GitHubTag = tag_with_version.tag
        version = tag_with_version.version
        commit_url = tag.url
        async with self._lock:
            try:
                response = await self._fetch_json_response(commit_url)
            except ResponseError:
                return Release(version)
        commit_info = response.get_content()
        release_date = fetch(commit_info, 'commit', 'committer','date', output_type=str).strip()[:10]
        return Release(version, release_date)

    async def _get_release_from_changelog(self, changelog_uri: str) -> Release | None:
        if 'blob/master' not in changelog_uri:
            return None
        path = urlparse(changelog_uri).path
        raw_path = path.replace('blob', 'refs/heads')
        host_raw = 'https://raw.githubusercontent.com'
        raw_changelog_uri = urljoin(host_raw, raw_path)
        async with self._lock:
            try:
                response = await self._fetch_text_response(raw_changelog_uri)
            except ResponseError:
                return None
        changelog = response.get_content()
        return await async_to_thread(self._semaphore, self._fetch_from_changelog, changelog)

    async def _load_project_info(self) -> None:
        async with self._lock:
            try:
                response = await self._fetch_json_response(self._project_url)
            except ResponseError:
                return
        self._project_info = response.get_content()
        self._membership_confirmed = bool(self._project_info)

    def _get_api_url_for_project(self) -> str:
        project_path = urlparse(self._url).path
        git_suffix_match = re.search(r'\.git/*$', project_path)
        if git_suffix_match is not None:
            project_path = project_path[:git_suffix_match.start()]
            self._url = str(urljoin('https://github.com/', project_path))
        return urljoin(self._api_base_url, f'/repos{project_path}').rstrip('/')

    async def _fetch_json_response(self, url, **kwargs) -> JsonResponse:
        custom_parameters = {
            'session_name': 'github'
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_json_response(url, **custom_parameters)

    async def _fetch_text_response(self, url, **kwargs) -> TextResponse:
        custom_parameters = {
            'session_name': 'github'
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_text_response(url, **custom_parameters)


class GitLabProject(GitProject):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        self._mode = 'GitLab'
        self._lock: Lock = self._recognition_context.synchronization.gitlab_lock
        self._api_base_url: str = f'https://{urlparse(self._url).hostname or 'gitlab.com'}/api/v4/'
        self._url_parse: ParseResult | None = None
        self._subdomain_owner: str | None = None
        self._project_url: str = self._get_api_url_for_project()
        self._readme_url: str = ''
        self._license_url: str = ''
        self._author: str = ''
        self._version_separators: list[str] = [r'\.', r'\-', r'_']
        self._project_info: Mapping = {}
        if version_info is None:
            self._version_info: VersionInfo | None = self._get_version_info()

    async def initialize(self) -> Self:
        await self._load_project_info()
        return self

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['gitlab.', 'invent.kde.']

    async def get_software(self) -> Brand:
        url_path = self._url_parse.path[1:]
        if not self._project_info:
            name = url_path.rsplit('/', 1)[:-1] if url_path else self._package_name
            return Brand(name)
        project_name = fetch(self._project_info, 'name', output_type=str).strip()
        name_from_path = fetch(self._project_info, 'path', output_type=str).strip()
        name = project_name or name_from_path
        other_names = {name_from_path, self._package_name}
        redundant = [name.lower(), '']
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)

    async def get_publisher(self) -> Brand:
        author = fetch(self._project_info, 'namespace', 'name', output_type=str).strip()
        name_from_path = fetch(self._project_info, 'namespace', 'path', output_type=str).strip()
        subdomain_owner = self._subdomain_owner
        if subdomain_owner is not None:
            name = author or name_from_path or subdomain_owner
            other_names = {name_from_path, subdomain_owner}
        else:
            name = author or name_from_path
            other_names = {name_from_path}
        redundant = [name.lower(), '']
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)

    def get_homepage(self) -> str:
        web_url = fetch(self._project_info, 'web_url', output_type=str).strip()
        self._homepage = web_url or self._url
        return self._homepage

    async def get_description(self) -> str:
        minimal_length = 10
        description = fetch(self._project_info, 'description', output_type=str).strip()
        if description:
            description_components: list[str] = [description]
            tag_list = fetch(self._project_info, 'tag_list', output_type=list)
            if tag_list:
                description_components.append(
                    f'The following list of tags corresponds to the functionality of software and '
                    f'can assist in formulating a more accurate definition of the software: {', '.join(tag_list)}'
                )
            combined_description = '\n'.join(description_components)
            if len(combined_description) >= minimal_length:
                self._description = combined_description
                return self._description
        elif len(self._description) >= minimal_length:
            return self._description
        readme_url = fetch(self._project_info, 'readme_url', output_type=str).strip()
        if not readme_url:
            return self._description
        readme_file_match = re.search(r'/blob/([^/]+)/(.+)$', readme_url)
        if readme_file_match is not None:
            ref = readme_file_match.group(1)
            readme_file_path = readme_file_match.group(2)
            readme_file_path_encoded = quote(readme_file_path, safe='')
            readme_api_url = f'{self._project_url}/repository/files/{readme_file_path_encoded}/raw?ref={ref}'
            async with self._lock:
                try:
                    response = await self._fetch_text_response(readme_api_url)
                except ResponseError:
                    return self._description
            readme: str = response.get_content().strip()
            if not readme:
                if description:
                    self._description = description
                return self._description
            readme_head = GitLabProject._get_text_head(readme)
            combined_description = '\n'.join(
                component for component in [description, readme_head] if component
            )
            if combined_description:
                self._description = combined_description
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        if not self._project_info:
            return self._license_info
        license_name = fetch(self._project_info, 'license', 'name', output_type=str).strip()
        license_key = fetch(self._project_info, 'license', 'key', output_type=str).strip()

        def is_valid(l: str) -> bool: return l.lower() not in ['', 'other']

        valid_license = next((l for l in [license_name, license_key] if is_valid(l)), None)
        if valid_license is not None:
            self._license_info = LicenseInfo([valid_license])
        if not self._license_info.content:
            license_url = fetch(self._project_info, 'license', 'license_url', output_type=str).strip()
            license_file_match = re.search(r'/blob/([^/]+)/(.+)$', license_url)
            if license_file_match is None:
                return self._license_info
            ref = license_file_match.group(1)
            license_file_path = license_file_match.group(2)
            license_file_path_encoded = quote(license_file_path, safe='')
            raw_license_url = f'{self._project_url}/repository/files/{license_file_path_encoded}/raw?ref={ref}'
            async with self._lock:
                try:
                    response = await self._fetch_text_response(raw_license_url)
                except ResponseError:
                    return self._license_info
            license_text = response.get_content()
            if not license_text:
                return self._license_info
            self._license_info = LicenseInfo([GitLabProject._get_text_head(license_text)], is_raw_text=True)
        return self._license_info

    async def get_release(self, standalone: bool = False, **kwargs: Any) -> Release:
        if self._version_info is None:
            return Release()
        version_is_date = self._version_info.is_date
        date_in_version = self._version_info.date_in_version
        release_date_from_version = date_in_version.iso_format() if version_is_date else ''
        if (tag_with_version := await self._fetch_tag_with_version()) is None:
            return Release(self._version_with_suffix, release_date_from_version)
        matched_tag, version = tag_with_version
        return Release(version, matched_tag.date[:10])

    @staticmethod
    def _get_text_head(content: str) -> str:
        content_lines = content.strip().splitlines()
        slice_index = 5
        following_count = None
        for ind, line in enumerate(content_lines[slice_index:]):
            if not line.strip():
                following_count = ind
                break
        slice_index = slice_index + following_count if following_count is not None else len(content_lines)
        return '\n'.join(content_lines[:slice_index])

    async def _load_project_info(self) -> None:
        self._resolve_subdomain_owner()
        await self._get_project_info()

    async def _get_project_info(self) -> None:
        params = {'license': 'yes'}
        async with self._lock:
            try:
                response = await self._fetch_json_response(self._project_url, params=params)
            except ResponseError:
                return
        self._project_info = response.get_content()
        self._membership_confirmed = bool(self._project_info)

    def _get_api_url_for_project(self) -> str:
        self._url_parse = urlparse(self._url)
        path = urlparse(self._url).path[1:]
        path_encoded = quote(path, safe='')
        return urljoin(self._api_base_url, f'projects/{path_encoded}')

    def _resolve_subdomain_owner(self) -> None:
        hostname = self._url_parse.hostname
        match_subdomain = re.search(r'\.([^.]+)\.org', hostname)
        self._subdomain_owner = match_subdomain.group(1) if match_subdomain is not None else None

    async def _fetch_json_response(self, url, **kwargs) -> JsonResponse:
        custom_parameters = {
            'session_name': 'gitlab'
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_json_response(url, **custom_parameters)

    async def _fetch_text_response(self, url, **kwargs) -> TextResponse:
        custom_parameters = {
            'session_name': 'gitlab'
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_text_response(url, **custom_parameters)


class PagureProject(Project):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        url_path = urlparse(self._url).path
        self._project_name: str = url_path.strip('/') or self._package_name
        self._all_info_known: bool = bool(self._description and self._license_info.content)
        self._pagure_package_instance: FedoraPackage = FedoraPackage(
            recognition_context, self._fingerprint, family='fedora', pagure_origin=True
        )
        if version_info is None:
            self._version_info: VersionInfo | None = self._get_version_info()

    async def initialize(self) -> Self:
        if self._all_info_known:
            self._membership_confirmed = True
            return self
        await self._pagure_package_instance.initialize()
        self._description = self._pagure_package_instance.get_description()
        self._license_info = self._pagure_package_instance.get_license_info()
        self._membership_confirmed = bool(self._description and self._license_info.content)
        return self

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['pagure.']

    async def get_software(self) -> Brand:
        name = self._project_name.title()
        redundant = [name.lower(), '']
        alternative_names = Project._resolve_names([self._package_name], redundant)
        return Brand(name, alternative_names)

    async def get_publisher(self) -> Brand:
        return Brand('Red Hat')

    def get_homepage(self) -> str:
        return self._url

    async def get_release(self, standalone: bool = False, **kwargs: Any) -> Release:
        if self._version_info is None:
            return Release()
        return Release(self._version_with_suffix)

    async def get_description(self) -> str:
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        return self._license_info


class MetaCPANProject(Project):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        self._version: str = fingerprint.version
        self._base_url: str = 'https://metacpan.org/'
        self._api_base_url: str = 'https://fastapi.metacpan.org/v1/'
        self._distribution_name: str = self._url.rsplit('/', 1)[-1]
        self._module_name: str = self._distribution_name.replace('-', '::')
        self._download_url: str = f'{self._api_base_url}download_url/{self._module_name}'
        self._version_separators: list[str] = [r'\.', r'\-', r'_']
        self._distribution_info: PerlDistributionInfo = PerlDistributionInfo()
        if version_info is None:
            self._version_info: VersionInfo | None = self._get_version_info()

    async def initialize(self) -> Self:
        await self._load_distribution_info()
        return self

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['metacpan.']

    async def get_software(self) -> Brand:
        distribution_data = self._distribution_info.distribution
        distribution_name = distribution_data.name
        standard_module_name = distribution_name.replace('-', '::')
        provided_modules = distribution_data.provides or []
        modules = {standard_module_name, *provided_modules}
        distro_variants = {f'perl-{distribution_name}', f'lib{distribution_name.lower()}-perl'}
        other_names = distro_variants | modules
        redundant = [distribution_name.lower(), '']
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(distribution_name, alternative_names)

    async def get_publisher(self) -> Brand:
        if self._distribution_name == 'perl':
            return Brand('Perl')
        author, author_abbr = astuple(self._distribution_info.author)
        name = author or author_abbr
        if not name:
            return Brand('CPAN', ['Comprehensive Perl Archive Network'])
        other_names = [author_abbr]
        redundant = [name.lower(), '']
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)

    def get_homepage(self) -> str:
        homepage = self._distribution_info.homepage
        return homepage if homepage else self._distribution_info.distribution.metacpan_html_url

    async def get_description(self) -> str:
        distribution = self._distribution_info.distribution
        modules = self._distribution_info.modules or []
        if distribution.name.lower() == 'perl':
            common = distribution.description
        else:
            common = f'{distribution.name} is a distribution of Perl modules.'
        if distribution.obsolete:
            module = self._module_name
            main = (f' that provided the {module} module. At the present time, the mentioned '
                    f'module is a part of {distribution.name_of_current} distribution.')
            module_info = next((m.description for m in modules if m.name == self._module_name), '')
            if module_info:
                module_info = f' The following is a description of the {module} module: {module_info}'
            self._description = ','.join([common, main, module_info])
            return self._description
        if distribution.provides:
            provides_info = f'The modules included are: {', '.join(m for m in distribution.provides)}.'
        elif self._distribution_name != 'perl':
            provides_info = f'One of the included modules is {modules[0].name if modules else self._module_name}.'
        else:
            provides_info = ''

        def merge(merged: str, m: PerlModule) -> str:
            if not m.description:
                return merged

            start = f'The following is a description of {m.name}, '
            significant = 'the main module of' if m.is_main else 'one of the modules within'
            last =' the distribution: '
            return f'{merged}\n{''.join([merged, start, significant, last, m.description.strip()])}'

        modules_description = f'{reduce(merge, modules, '')}'
        description_components = [
            f'{common}\n{provides_info}'.strip(), modules_description, distribution.abstract
        ]
        self._description = '\n'.join(component for component in description_components if component)
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        raw_licenses = self._distribution_info.licenses if self._distribution_info.licenses else ['perl_5']
        licenses = []
        for item in raw_licenses:
            if item.lower() == 'perl_5':
                licenses.append('Artistic License 1.0')
                licenses.append('GNU General Public License v1.0 only')
            else:
                licenses.append(item)
        self._license_info = LicenseInfo(licenses)
        return self._license_info

    async def get_release(self, standalone: bool = False, **kwargs: Any) -> Release:
        if standalone:
            await self._load_release_specific_info()
        if self._version_info is None:
            return Release()
        if self._distribution_info.required_release is not None:
            version = self._distribution_info.required_release.version
            iso_date = self._distribution_info.required_release.date[:10]
            return Release(version, iso_date)
        distribution_name = self._distribution_info.distribution.name
        author_abbr = self._distribution_info.author.abbr
        if author_abbr:
            release_name = f'{distribution_name}-{self._version}'
            required_release_url = f'{self._api_base_url}release/{author_abbr}/{release_name}'
            response = await self._fetch_json_response_safe(required_release_url)
            if response is not None:
                content = response.get_content()
                date = fetch(content, 'date', output_type=str).strip()
                if date:
                    return Release(self._version_with_suffix, date[:10])
        changelog_url = f'{self._api_base_url}changes/{self._distribution_info.distribution.name_of_current}'
        response = await self._fetch_json_response_safe(changelog_url)
        if response is not None:
            content = response.get_content()
            changelog = fr'{fetch(content, 'content', output_type=str)}'
            return await async_to_thread(self._semaphore, self._fetch_from_changelog, changelog)
        latest_release = self._distribution_info.latest_release
        if latest_release is None or not latest_release.changes_url:
            return Release(self._version_with_suffix)
        response = await self._fetch_text_response_safe(latest_release.changes_url)
        if response is None:
            return Release(self._version_with_suffix)
        changelog = response.get_content()
        return self._fetch_from_changelog(changelog)

    async def _load_distribution_info(self) -> None:
        await self._load_download_info()
        await self._load_module_info()
        if self._distribution_info.distribution is None:
            return
        await self._load_release_info()
        self._membership_confirmed = True
        metacpan_html_url = f'{self._base_url}dist/{self._distribution_info.distribution.name_of_current}'
        self._distribution_info.distribution.metacpan_html_url = metacpan_html_url
        latest_release = self._distribution_info.latest_release
        if latest_release is not None and self._distribution_info.required_release is None and latest_release.date:
            if latest_release.version == self._version:
                self._distribution_info.required_release = latest_release

    async def _load_download_info(self) -> None:
        download_url = self._download_url + (f'?version==={self._version}' if self._version else '')
        response = await self._fetch_json_response_safe(download_url)
        if response is None:
            return
        release_info = response.get_content()
        distribution_name = fetch(release_info, 'distribution', output_type=str).strip()
        if distribution_name:
            self._distribution_name = distribution_name
            distribution = PerlDistribution(distribution_name, name_of_current=distribution_name)
            self._distribution_info.distribution = distribution
            if distribution_name.lower() != 'perl':
                module = PerlModule(self._module_name)
                module.metacpan_url = f'{self._base_url}pod/{self._module_name}'
                self._distribution_info.modules = [module]
            else:
                self._module_name = 'perl'
        if not self._version:
            return
        version = fetch(release_info, 'version', output_type=str)
        if self._version not in version:
            required_release = PerlRelease('unknown')
            self._distribution_info.required_release = required_release
            return
        date = fetch(release_info, 'date', output_type=str).strip()
        if date:
            required_release = PerlRelease(self._version, date)
            self._distribution_info.required_release = required_release

    async def _load_module_info(self, module_name: str | None = None) -> None:
        if module_name is None:
            module_name = self._module_name
        module_url = f'{self._api_base_url}module/{module_name}'
        response = await self._fetch_json_response_safe(module_url)
        if response is None:
            return
        module_info = response.get_content()
        distribution = self._distribution_info.distribution
        description = fetch(module_info, 'description', output_type=str).strip()
        if module_name == 'perl':
            if distribution is not None:
                distribution.description = description
            return
        modules = self._distribution_info.modules or []
        if module_name not in [m.name for m in modules]:
            module = PerlModule(module_name)
            module.metacpan_url = f'{self._base_url}pod/{module_name}'
            modules.append(module)
            module_index = len(modules) - 1
        else:
            module_index = next(ind for ind, m in enumerate(modules) if m.name == module_name)
        modules[module_index].description = description
        current_distribution_name = fetch(module_info, 'distribution', output_type=str).strip()
        if current_distribution_name:
            self._distribution_name = current_distribution_name
            if distribution is None:
                self._distribution_info.distribution = PerlDistribution(
                    current_distribution_name, name_of_current=current_distribution_name
                )
            elif current_distribution_name != self._distribution_info.distribution.name:
                distribution.obsolete = True
                distribution.name_of_current = current_distribution_name
        author_abbr = fetch(module_info, 'author', output_type=str).strip()
        if author_abbr:
            author = PerlAuthor(abbr=author_abbr)
            self._distribution_info.author = author
        if self._distribution_info.required_release is None:
            latest_version = fetch(module_info, 'version_numified', output_type=str).strip()
            if latest_version not in ['', '0']:
                self._distribution_info.latest_release = PerlRelease(latest_version)

    async def _load_release_info(self) -> None:
        release_api_url = self._get_release_api_url()
        response = await self._fetch_json_response_safe(release_api_url)
        if response is None:
            return
        release_info = response.get_content()
        main_module = fetch(release_info, 'main_module', output_type=str).strip()
        if self._distribution_info.modules and self._distribution_info.modules[0].name == main_module:
            self._distribution_info.modules[0].is_main = True
        distribution = self._distribution_info.distribution
        if not distribution.obsolete:
            abstract = fetch(release_info, 'abstract', output_type=str).strip()
            if abstract not in ['', 'unknown']:
                self._distribution_info.distribution.abstract = abstract
            provides = fetch(release_info, 'provides')
            if provides:
                if isinstance(provides, list):
                    self._distribution_info.distribution.provides = [m for m in provides if isinstance(m, str)]
                elif isinstance(provides, str):
                    self._distribution_info.distribution.provides = [provides]
        if self._distribution_info.author is None:
            author_abbr = fetch(release_info, 'author', output_type=str)
            self._distribution_info.author = PerlAuthor(abbr=author_abbr)
        author_info = fetch(release_info, 'metadata', 'author', output_type=list)
        if author_info:
            self._parse_author_info(author_info)
        licenses = fetch(release_info, 'license', output_type=list)
        if licenses not in [['unknown'], []]:
            self._distribution_info.licenses = licenses
        self._distribution_info.homepage = fetch(release_info, 'resources','homepage', output_type=str)
        if self._distribution_info.required_release is not None:
            return
        latest_version = fetch(release_info, 'version').strip()
        if self._distribution_info.latest_release is None:
            self._distribution_info.latest_release = PerlRelease(version=latest_version)
        latest_date = fetch(release_info, 'date', output_type=str)
        self._distribution_info.latest_release.date = latest_date
        abbr = self._distribution_info.author.abbr
        changes_file = fetch(release_info, 'changes_file',  output_type=str)
        if abbr and changes_file and changes_file != 'unknown':
            changes_path = f'source/{abbr}/{distribution.name}-{latest_version}'
            self._distribution_info.latest_release.changes_url = f'{self._api_base_url}{changes_path}'

    def _parse_author_info(self, author_info: list[str]) -> None:
        if not isinstance(author_info[0], str):
            return
        raw_authors = author_info[0].split(',')
        authors = []
        for raw_author in raw_authors:
            author = re.sub(r'<[^<>]+>', '', raw_author).strip()
            if author:
                authors.append(author)
        if not authors:
            return
        authors_str = ', '.join(sorted(authors))
        self._distribution_info.author.name = authors_str

    async def _load_release_specific_info(self) -> None:
        self._version_info = self._get_version_info()
        await self._load_download_info()
        if isinstance(self._distribution_info.required_release, PerlRelease):
            return
        await self._load_module_info()
        await self._load_release_info()

    def _get_release_api_url(self):
        return f'{self._api_base_url}release/{self._distribution_name}'

    async def _fetch_json_response_safe(self, url: str) -> JsonResponse | None:
        try:
            return await self._fetch_json_response(url)
        except ResponseError:
            return None

    async def _fetch_text_response_safe(self, url: str) -> TextResponse | None:
        try:
            return await self._fetch_text_response(url)
        except ResponseError:
            return None


class SourceForgeProject(Project):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        self._headers: dict[str, str] = {
            'Authorization': f'Bearer {getenv('LINUX_RECOGNITION__SOURCEFORGE_BEARER')}'
        }
        self._base_url: str = 'https://sourceforge.net/'
        self._base_api_url: str = f'{self._base_url}rest/'
        self._project_name: str = self._get_project_name()
        self._rss_url: str = f'{self._base_url}projects/{self._project_name}/rss?limit=999999'
        self._version_separators: list[str] = [r'\.', r'\-', r'_']
        self._project_info: Mapping = {}
        if version_info is None:
            self._version_info: VersionInfo | None = self._get_version_info()

    async def initialize(self) -> Self:
        await self._load_project_info()
        return self

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['sourceforge.']

    async def get_software(self) -> Brand:
        fullname = fetch(self._project_info, 'name', output_type=str).strip()
        shortname = fetch(self._project_info, 'shortname', output_type=str).strip()
        project_name = self._project_name.title()
        name = fullname or shortname or project_name
        other_names = {shortname, project_name, self._package_name}
        redundant = [name.lower(), '']
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)

    async def get_publisher(self) -> Brand:
        source_forge_publisher = Brand('SourceForge', ['SourceForge.net'])
        developers = fetch(self._project_info, 'developers', output_type=list)
        if not developers:
            return source_forge_publisher
        if len(developers) == 1:
            developer = developers[0]
            if not isinstance(developer, Mapping):
                return source_forge_publisher
            fullname = fetch(developer, 'name', output_type=str).strip()
            username = fetch(developer, 'username', output_type=str).strip()
            if not fullname and not username:
                return source_forge_publisher
            name = fullname or username
            alternative_names = [username] if username.lower() not in ['', name.lower()] else []
            return Brand(name, alternative_names)
        authors = []
        for developer in developers:
            if not isinstance(developer, Mapping):
                continue
            fullname = fetch(developer, 'name', output_type=str).strip()
            username = fetch(developer, 'username', output_type=str).strip()
            name = fullname or username
            if name:
                authors.append(name)
        if not authors:
            return source_forge_publisher
        publisher = ', '.join(sorted(authors))
        return Brand(publisher)

    def get_homepage(self) -> str:
        self._homepage = self._url
        external_homepage = fetch(self._project_info, 'external_homepage', output_type=str).strip()
        if external_homepage and 'sourceforge' not in external_homepage.lower():
            self._homepage = external_homepage
        return self._homepage

    async def get_description(self) -> str:
        description = fetch(self._project_info, 'short_description', output_type=str).strip()
        self._description = description or self._description
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        license_data = fetch(self._project_info, 'categories', 'license', output_type=list)
        if not license_data:
            return self._license_info
        licenses = []
        for item in license_data:
            license_name = fetch(item, 'fullname', output_type=str).strip() or (
                fetch(item, 'shortname', output_type=str)).strip()
            if license_name:
                licenses.append(license_name)
        if licenses:
            self._license_info = LicenseInfo(licenses)
        return self._license_info

    async def get_release(self, standalone: bool = False, **kwargs: Any) -> Release:
        if self._version_info is None:
            return Release()
        rss_url = f'{self._base_url}projects/{self._project_name}/rss?limit=999999'
        return await self._fetch_release_from_rss(rss_url)

    def _scan_element(self, element: Element, partial_matches: list[ReleaseInfo]) -> ReleaseInfo | None:
        general_pattern = self._version_info.pattern.general
        exact_pattern = self._version_info.pattern.exact
        scanned_tag = element.find('title')
        if scanned_tag is None:
            scanned_tag = element.find('description')
            if scanned_tag is None:
                return None
        segments = scanned_tag.text.split('/')
        for part in segments:
            match = general_pattern.search(part)
            if match is not None:
                logger.debug('RSS feed scan - matched item', extra={'matched+item': part})
                date_published = element.find('pubDate')
                if date_published is None:
                    return None
                date_info = date_published.text.strip()
                if exact_match := exact_pattern.search(part) is not None:
                    return ReleaseInfo(match=exact_match, date_info=date_info)
                tail = self._get_version_tail(part[match.end():])
                release_item = ReleaseInfo(match=match, date_info=date_info, tail=tail)
                partial_matches.append(release_item)
                return None
        return None

    async def _load_project_info(self) -> None:
        if not self._project_name:
            return
        project_api_url = f'{self._base_api_url}p/{self._project_name}'
        try:
            response = await self._fetch_json_response(project_api_url)
        except ResponseError:
            return
        self._project_info = response.get_content()
        self._membership_confirmed = bool(self._project_info)

    def _get_project_name(self) -> str:
        url_pattern = r'sourceforge\.net/(?:projects|p)/(?P<proj>[^/]+)|(?P<subdomain>[^/]+)(?=\.sourceforge\.net)'
        match = re.search(url_pattern, self._url)
        project_match = match.group('proj')
        return project_match if project_match is not None else match.group('subdomain') or self._package_name

    async def _fetch_json_response(self, url, **kwargs) -> JsonResponse:
        custom_parameters = {
            'headers': self._headers
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_json_response(url, **custom_parameters)


class PyPIProject(Project):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        self._headers: dict[str, str] = {'User-Agent': getenv('LINUX_RECOGNITION__PYPI_USER_AGENT')}
        self._base_url: str = 'https://pypi.org/'
        self._project_name: str = PyPIProject._fetch_project_name_from_url(self._url) or self._package_name
        self._url = f'{self._base_url}project/{self._project_name}'
        self._project_api_url: str = f'{self._base_url}pypi/{self._project_name}/json' if self._project_name else ''
        self._releases_feed_url: str = f'{self._base_url}rss/project/{self._project_name}/releases.xml'
        self._version_separators: list[str] = [r'\.', r'\-', r'_']
        self._project_info: Mapping = {}
        if version_info is None:
            self._version_info: VersionInfo | None = self._get_version_info()

    async def initialize(self) -> Self:
        await self._load_project_info()
        return self

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['pypi.']

    async def get_software(self) -> Brand:
        redundant = [self._project_name.lower(), '']
        alternative_names = Project._resolve_names([self._package_name], redundant)
        return Brand(self._project_name, alternative_names)

    async def get_publisher(self) -> Brand:
        author = fetch(self._project_info, 'info', 'author', output_type=str).strip()
        if not self._homepage:
            return Brand(author)
        publisher_from_url = ''
        homepage_hostname = urlparse(self._homepage).hostname or ''
        if 'github' in homepage_hostname:
            github_author_pattern = r'github\.com/([^/]+)/'
            match = re.search(github_author_pattern, self._homepage)
            if match is not None:
                publisher_from_url = match.group(1)
        elif 'gitlab' in homepage_hostname:
            gitlab_author_pattern = r'gitlab\.com/(?P<proj>[^/]+)|gitlab.(?P<subdomain>[^.]+)\.org'
            match = re.search(gitlab_author_pattern, self._homepage)
            if match is not None:
                publisher_from_url = match.group('proj') or match.group('subdomain')
        name = author or publisher_from_url.title()
        other_names = [publisher_from_url.title()]
        redundant = [name.lower(), '']
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name, alternative_names)

    def get_homepage(self) -> str:
        return self._homepage or self._url

    async def get_description(self) -> str:
        if not self._project_info:
            return self._description

        def is_relevant(line: str) -> bool:
            line_core = line.strip()
            if not line_core:
                return False
            link_match = re.search(r'https?:', line_core)
            return True if not link_match else False

        summary_text = fetch(self._project_info, 'info', 'summary', output_type=str)
        summary = f'Summary: {summary_text}' if summary_text else ''
        raw_description = fetch(self._project_info, 'info', 'description', output_type=str)
        description_lines = [line.strip() for line in raw_description.splitlines() if is_relevant(line)][:25]
        description = f'Description: {' '.join(description_lines)}' if description_lines else ''
        project_description = '\n'.join([p for p in (summary, description) if p])
        if project_description:
            self._description = project_description
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        license_info = fetch(self._project_info, 'info', 'license', output_type=str)
        if license_info:
            self._license_info = LicenseInfo([license_info])
        return self._license_info

    async def get_release(self, standalone: bool = False, **kwargs: Any) -> Release:
        if self._version_info is None:
            return Release()
        release = await async_to_thread(self._semaphore, self._scan_releases_field)
        if isinstance(release, Release):
            return release
        rss_url = f'{self._base_url}rss/project/{self._project_name}/releases.xml'
        return await self._fetch_release_from_rss(rss_url)

    async def _load_project_info(self) -> None:
        try:
            response = await self._fetch_json_response(self._project_api_url)
        except ResponseError:
            return
        self._project_info = response.get_content()
        self._homepage = (
                fetch(self._project_info, 'info', 'home_page', output_type=str).strip()
                or fetch(self._project_info, 'info', 'project_urls', 'Homepage', output_type=str).strip()
                or fetch(self._project_info, 'info', 'project_url', output_type=str).strip()
                or self._url
        )
        self._membership_confirmed = bool(self._project_info)

    @staticmethod
    def _fetch_project_name_from_url(url) -> str | None:
        pattern = r'(?:pypi\.org/project|python\.org/pypi)/([^/]+)'
        project_name_match = re.search(pattern, url)
        if project_name_match is not None:
            return project_name_match.group(1)
        return None

    def _scan_releases_field(self) -> Release | None:
        exact_pattern = self._version_info.pattern.exact
        releases_info = fetch(self._project_info, 'releases', output_type=dict)
        if not releases_info:
            return None
        releases = releases_info.keys()
        matching_release = None
        for release in releases:
            if exact_pattern.search(release) is not None:
                matching_release = release
                break
        if matching_release is None:
            return None
        metadata = releases_info[matching_release]
        for file in metadata:
            iso_date = fetch(file, 'upload_time', output_type=str).strip()
            if iso_date:
                return Release(self._version_with_suffix, iso_date[:10])
        return None

    async def _fetch_json_response(self, url, **kwargs) -> JsonResponse:
        custom_parameters = {
            'headers': self._headers
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_json_response(url, **custom_parameters)


class RubyGemProject(Project):

    is_source: ClassVar[bool] = False
    _query: str

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint, url: str = '',
            package_instance: LinuxPackage = None,
            version_info: VersionInfo | None = None,
            query: str = ''
    ) -> None:
        super().__init__(recognition_context, fingerprint, url, package_instance, version_info)
        if not query:
            api_url_match = re.search(r'api/v1/search\.json\?query=(.+)', url)
            if api_url_match is not None:
                self._query = api_url_match.group(1)
            else:
                name_match = re.search('gems/([^/]+)', url)
                self._query = name_match.group(1) if name_match is not None else ''
        else:
            self._query= query
        self._base_url: str = 'https://rubygems.org/'
        self._search_api_url: str = f'{self._base_url}api/v1/search.json?query={self._query}'
        self._homepage: str = ''
        self._abstract: str = ''
        self._project_info: Mapping = {}
        if version_info is None:
            self._version_info = self._get_version_info()

    async def initialize(self) -> Self:
        await self._load_project_info()
        return self

    @classmethod
    def get_url_keys(cls) -> list[str]:
        return ['rubgems.']

    async def get_software(self) -> Brand:
        name_formatted = self._fetch_formatted_name()
        other_names = {self._query, self._package_name, f'rubygem-{self._query}'}
        redundant = ['', name_formatted.lower()]
        alternative_names = Project._resolve_names(other_names, redundant)
        return Brand(name_formatted, alternative_names)

    async def get_publisher(self) -> Brand:
        project_class = url_to_project(self._homepage, only_source=True)
        if project_class is not None:
            project: Project = project_class(
                self._recognition_context,
                self._fingerprint,
                url=self._homepage,
                version_info=self._version_info
            )
            await project.initialize()
            return await project.get_publisher()
        authors = fetch(self._project_info, 'authors', output_type=str).strip()
        return Brand(authors)

    def get_homepage(self) -> str:
        return self._homepage

    async def get_description(self) -> str:
        description = self._abstract or self._description
        if not description:
            project_class = url_to_project(self._homepage, only_source=True)
            if project_class is None:
                return self._description
            project: Project = project_class(
                self._recognition_context,
                self._fingerprint,
                url=self._homepage,
                version_info=self._version_info
            )
            await project.initialize()
            description = await project.get_description()
        self._description = description
        return self._description

    async def get_license_info(self) -> LicenseInfo:
        licenses = fetch(self._project_info, 'licenses', output_type=list)
        if licenses:
            self._license_info = LicenseInfo(licenses)
        return self._license_info

    async def get_release(self, standalone: bool = False, **kwargs: Any) -> Release:
        if self._version_info is None:
            return Release()
        changelog_uri = fetch(self._project_info, 'changelog_uri', output_type=str)
        if 'github.com' in changelog_uri:
            repo_url_pattern = re.compile(r'github\.com/[^/]+/[^/]+')
            repo_url_match = repo_url_pattern.search(self._homepage) or repo_url_pattern.search(changelog_uri)
            if repo_url_match is not None:
                repo_url = f'https://{repo_url_match.group()}'
                github = GitHubProject(
                    self._recognition_context,
                    self._fingerprint,
                    url=repo_url,
                    version_info=self._version_info
                )
                return await github.get_release(standalone=True, changelog_uri=changelog_uri)
        project_class = url_to_project(self._homepage, only_source=True)
        if project_class is None:
            return Release(self._version_with_suffix)
        project: Project = project_class(
            self._recognition_context,
            self._fingerprint,
            url=self._homepage,
            version_info=self._version_info
        )
        return await project.get_release(standalone=True)

    async def _load_project_info(self) -> None:
        try:
            response = await self._fetch_json_response(self._search_api_url)
        except ResponseError:
            return
        content = response.get_content()
        self._project_info = content[0] if (content and isinstance(content, list)) else {}
        self._membership_confirmed = bool(self._project_info)
        self._fetch_homepage()

    def _fetch_homepage(self) -> None:
        if not self._project_info:
            return
        self._homepage = (
                fetch(self._project_info, 'homepage_uri', output_type=str).strip() or
                fetch(self._project_info, 'project_uri', output_type=str) or
                f'{self._base_url}/gems/{self._query}'
        )

    def _fetch_formatted_name(self) -> str:
        if not self._project_info:
            return self._query
        separator = r'[\W_]+'
        name_split = re.split(separator, self._query)
        self._abstract = fetch(self._project_info, 'info', output_type=str).strip()
        name_pattern = separator.join(name_split)
        name_match = re.search(name_pattern, self._abstract, re.IGNORECASE)
        return name_match.group() if name_match is not None else self._query


def get_supported_projects() -> list[type[Project]]:
    def is_class(member: Any) -> bool: return isclass(member) and member.__module__ == __name__
    classes = getmembers(sys.modules[__name__], is_class)
    return [cls[1] for cls in classes]


def is_host_supported(url: str) -> bool:
    if not url.startswith('http'):
        url = f'https://{url}'
    hostname = urlparse(url).hostname or ''
    projects = get_supported_projects()
    url_keys = [key for project in projects for key in project.get_url_keys()]
    return any(key in hostname for key in url_keys)


def url_to_project(url: str, only_source: bool = False) -> type(Project) | None:
    if not url.startswith('http'):
        url = f'https://{url}'
    url_parse = urlparse(url)
    hostname = url_parse.hostname
    path = url_parse.path
    if hostname is None or not path:
        return None
    projects = [r for r in get_supported_projects() if r.is_source] if only_source else get_supported_projects()
    key_to_project = {key: project for project in projects for key in project.get_url_keys()}
    for key in key_to_project:
        if key in hostname:
            return key_to_project[key]
    return None
