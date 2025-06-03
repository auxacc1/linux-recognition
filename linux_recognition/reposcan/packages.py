import re
from base64 import b64encode
from collections.abc import Iterable, Mapping
from logging import getLogger
from os import getenv
from typing import Any, Self
from urllib.parse import urlparse, urljoin
from xml.etree.ElementTree import Element

from bs4 import BeautifulSoup
from defusedxml.ElementTree import fromstring

from db.postgresql.alpine import fetch_alpine_package_info
from db.postgresql.repology import fetch_package_info
from db.postgresql.udd import UDD
from reposcan.spec import Spec, replace_macros
from synchronization import async_to_thread
from typestore.datatypes import LicenseInfo, Fingerprint, PackageTools, Package, RecognitionContext
from typestore.errors import ResponseError
from webtools.content import fetch
from webtools.response import JsonResponse, TextResponse


logger = getLogger(__name__)


class LinuxPackage(Package):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        self._recognition_context = recognition_context
        self._fingerprint = fingerprint
        self._family = family
        self._is_host_supported = recognition_context.is_host_supported
        self._session_manager = recognition_context.session_handler
        self._db_pools = recognition_context.source_db_pools
        self._semaphore = recognition_context.synchronization.semaphore
        self._raw_name = fingerprint.software.replace(' ', '-')
        self._name: str = ''
        self._description: str = ''
        self._license_info: LicenseInfo = LicenseInfo(content=[])
        self._homepage: str  = ''
        self._package_url: str  = ''
        self._vendor: str = ''
        self._name_normalized: bool = False

    async def initialize(self) -> Self:
        return self

    def get_name(self) -> str:
        if not self._name_normalized:
            self._normalize_name()
        return self._name

    def get_package_url(self) -> str:
        return self._package_url

    def get_vendor(self) -> str:
        return self._vendor

    def get_homepage(self) -> str:
        return self._homepage

    def get_description(self) -> str:
        return self._description

    def get_license_info(self) -> LicenseInfo:
        return self._license_info

    async def get_data_for_universal_package(self) -> str:
        universal_package = await UniversalPackage(
            self._recognition_context, self._fingerprint, self._family
        ).initialize()
        self._homepage = universal_package.get_homepage()
        if self._homepage:
            logger.info('Retrieved from Repology database', extra={'raw_name': self._raw_name})
            self._name = universal_package.get_name()
            self._description = universal_package.get_description()
            self._license_info = universal_package.get_license_info()
            self._package_url = self._homepage
        return self._homepage

    def _parse_spec_content(self, spec_file_content: str) -> None:
        spec = Spec.from_string(spec_file_content)
        if hasattr(spec, 'srcname'):
            package_name = spec.srcname
        elif hasattr(spec, 'package_name'):
            package_name = spec.package_name
        elif hasattr(spec, 'name'):
            package_name = spec.name
        else:
            package_name = ''
        name = replace_macros(package_name, spec)
        self._name = name or self._raw_name
        description_components = []
        if hasattr(spec, 'summary'):
            summary = (replace_macros(spec.summary, spec) or '').strip()
            if summary:
                description_components.append(summary)
        if hasattr(spec, 'description'):
            description = (replace_macros(spec.description, spec) or '').strip()
            if description:
                description_components.append(description)
        self._description = '\n'.join(description_components)
        if hasattr(spec, 'license'):
            self._license_info = LicenseInfo([spec.license])
        if hasattr(spec, 'url') and spec.url:
            self._package_url = self._homepage = spec.url.strip()

    def _normalize_name(self) -> None:
        name = self._name
        version = self._fingerprint.version
        max_suffix_length = min(len(name), len(version))
        start_index = max(0, len(name) - max_suffix_length)
        for ind in range(start_index, len(name)):
            suffix = name[ind:]
            if version.startswith(suffix):
                self._name = self._name[:ind].rstrip(' -')
                self._name_normalized = True
                break

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


class DebianBasedPackage(LinuxPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)

    def _normalize_name(self) -> None:
        super()._normalize_name()
        lib_pattern = r'^lib(?!rary|ert|erat|ellous)-?'
        self._name = re.sub(lib_pattern, '', self._name, flags=re.IGNORECASE)


class DebianPackage(DebianBasedPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None,
            fetch_licenses: bool = False,
            fetch_description: bool = False
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._fetch_licenses = fetch_licenses
        self._fetch_description = fetch_description
        self._sources_url_base = 'https://sources.debian.org/'
        self._vendor = 'Debian'
        self._raw_control_url = ''
        self._license_url = ''

    async def initialize(self) -> Self:
        self._homepage = await self.get_data_for_universal_package()
        if self._homepage:
            if not self._description or not self._license_info.content:
                await self._load_remaining_data()
            return self
        udd = UDD(
            self._raw_name,
            self._db_pools.udd,
            self._recognition_context.project_directory,
            udd_lock=self._recognition_context.synchronization.udd_lock
        )
        self._homepage = await udd.get_homepage()
        self._name = udd.get_source_package()
        if not self._name:
            udd = UDD(
                self._raw_name,
                self._db_pools.udd,
                self._recognition_context.project_directory,
                udd_lock=self._recognition_context.synchronization.udd_lock,
                packages_table='archived_packages',
                sources_table='archived_sources'
            )
            self._homepage = await udd.get_homepage()
            self._name = udd.get_source_package()
            if not self._name:
                return self
        if self._homepage:
            self._package_url = self._homepage
        await self._load_remaining_data()
        return self

    async def _load_remaining_data(self) -> None:
        if self._package_url and not self._fetch_licenses and not self._fetch_description:
            return
        await self._fetch_relevant_urls()
        if self._fetch_licenses:
            await self._retrieve_license_info()
        if self._fetch_description:
            await self._load_description()

    async def _fetch_relevant_urls(self) -> None:
        control_api_url = f'{self._sources_url_base}api/src/{self._name}/latest/debian/control/'
        try:
            response = await self._fetch_json_response(control_api_url)
        except ResponseError:
            return
        content = response.get_content()
        raw_url = fetch(content, 'raw_url', output_type=str).strip()
        if raw_url:
            self._raw_control_url = urljoin(self._sources_url_base, raw_url)
        if 'pkg_infos' not in content:
            return
        pkg_infos = content['pkg_infos']
        license_path = fetch(pkg_infos, 'license', output_type=str).strip()
        if license_path:
            self._license_url = urljoin(self._sources_url_base, license_path)
        if not self._package_url:
            pts_link = fetch(pkg_infos, 'pts_link', output_type=str).strip()
            self._package_url = pts_link

    async def _retrieve_license_info(self) -> None:
        if not self._license_url:
            return
        try:
            response = await self._fetch_text_response(self._license_url)
        except ResponseError:
            return
        content = response.get_content()
        await async_to_thread(
            self._recognition_context.synchronization.semaphore, self._parse_copyright_content, content
        )

    def _parse_copyright_content(self, content: str) -> None:
        soup = BeautifulSoup(content, 'lxml')
        copyright_info_element = soup.find(id='copyright_info')
        if copyright_info_element is None:
            return
        files_fields = copyright_info_element.select('table > tr > td')  # malformed HTML, only top row is valid
        if len(files_fields) >= 3:
            common_license = files_fields[2].text.strip()
            if common_license:
                self._license_info = LicenseInfo([common_license])

    async def _load_description(self) -> None:
        if not self._raw_control_url:
            return
        try:
            response = await self._fetch_text_response(self._raw_control_url)
        except ResponseError:
            return
        content = response.get_content()
        index_0 = 0
        found = False
        pacakge_description = ''
        control_lines = content.splitlines()
        for index, line in enumerate(control_lines):
            if 'description:' in line.lower():
                found = True
                index_0 = index + 1
                continue
            if found:
                if not re.search(r'\w+', line):
                    index_1 = index
                    pacakge_description = '\n'.join(control_lines[index_0:index_1]).strip()
                    break
        self._description = pacakge_description


class UbuntuPackage(DebianBasedPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._vendor = 'Canonical'
        self._launchpad_source_base = 'https://launchpad.net/ubuntu/+source/'

    async def initialize(self) -> Self:
        self._homepage = await self.get_data_for_universal_package()
        if self._homepage:
            return self
        udd = UDD(
            self._raw_name,
            self._db_pools.udd,
            self._recognition_context.project_directory,
            udd_lock=self._recognition_context.synchronization.udd_lock
        )
        self._homepage = await udd.get_homepage()
        self._name = udd.get_source_package()
        if not self._name:
            udd = UDD(
                self._raw_name,
                self._db_pools.udd,
                self._recognition_context.project_directory,
                udd_lock=self._recognition_context.synchronization.udd_lock,
                packages_table='archived_packages',
                sources_table='archived_sources'
            )
            self._homepage = await udd.get_homepage()
            self._name = udd.get_source_package()
            if not self._name:
                return self
        self._package_url = self._homepage or self._launchpad_source_base + self._name
        return self


class FedoraPackage(LinuxPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None,
            pagure_origin: bool = False
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._pagure_origin = pagure_origin
        self._pagure_base_url = 'https://pagure.io/'
        if self._pagure_origin:
            self._base_url = self._pagure_base_url
        else:
            self._base_url = 'https://packages.fedoraproject.org/'
        self._query_url = f'{self._base_url}search?query={self._raw_name}'
        self._src_base_url = 'https://src.fedoraproject.org/'
        self._api_base_url = self._pagure_base_url if self._pagure_origin else self._src_base_url
        self._vendor = 'Red Hat' if self._pagure_origin else 'Fedora'

    async def initialize(self) -> Self:
        self._homepage = await self.get_data_for_universal_package()
        if self._homepage:
            return self
        spec_file_url = await self._get_spec_file_url()
        if not spec_file_url:
            return self
        try:
            response = await self._fetch_text_response(spec_file_url)
        except ResponseError:
            return self
        spec_file_content = response.get_content()
        await async_to_thread(self._semaphore, self._parse_spec_content, spec_file_content)
        return self

    async def _get_spec_file_url(self) -> str:
        list_files_path = self._get_list_files_path(self._raw_name)
        list_files_url = urljoin(self._api_base_url, list_files_path)
        try:
            response = await self._fetch_json_response(
                list_files_url,
                treat_http_client_error_as_warning=True
            )
        except ResponseError:
            if self._pagure_origin:
                return ''
            source_package_names = await self._fetch_source_package_names()
            for name in source_package_names:
                list_files_path = self._get_list_files_path(name)
                list_files_url = urljoin(self._api_base_url, list_files_path)
                try:
                    response = await self._fetch_json_response(list_files_url)
                except ResponseError:
                    continue
                files_list_response: Mapping[str, Any] = response.get_content()
                return await async_to_thread(
                    self._semaphore, self._parse_files_list_response, files_list_response, name
                )
            return ''
        files_list_response: Mapping[str, Any] = response.get_content()
        return await async_to_thread(
            self._semaphore, self._parse_files_list_response, files_list_response, self._raw_name
        )

    def _parse_files_list_response(self, files_list_response: Mapping, name: str) -> str:
        files = fetch(files_list_response, 'content', output_type=list)
        if not files:
            return ''
        self._name = name
        if self._pagure_origin:
            package_url_path = f'/{self._name}'
        else:
            package_url_path =  f'/pkgs/{self._name}'
        self._package_url = urljoin(self._base_url, package_url_path)
        spec_file_url = ''
        for file in files:
            spec_pattern = r'\.spec$'
            content_url = fetch(file, 'content_url', output_type=str)
            if re.search(spec_pattern, content_url):
                spec_file_url = content_url
                break
        return spec_file_url

    async def _fetch_source_package_names(self) -> list[str]:
        try:
            response = await self._fetch_text_response(self._query_url)
        except ResponseError:
            return []
        content = response.get_content()
        return await async_to_thread(self._semaphore, self._parse_search_results, content)

    def _parse_search_results(self, content: str) -> list[str]:
        source_package_names = []
        soup = BeautifulSoup(content, features='lxml')
        did_you_mean_element = soup.find(
            lambda tag: tag.name == 'p' and 'Did you mean' in tag.text and tag.find('a')
        )
        if did_you_mean_element is not None:
            return source_package_names
        result_elements = soup.find_all(
            lambda t: t.name == 'a'
                      and t.get('href', '')
                      and 'position-relative' in t.parent.parent.get('class', [])
        )
        for element in result_elements:
            if self._raw_name == element.text.strip():
                source_package_pattern = r'/pkgs/([^/]+)/'
                source_package_match = re.search(source_package_pattern, element['href'])
                if source_package_match is not None:
                    source_package_names.append(source_package_match.group(1))
        return source_package_names

    def _get_list_files_path(self, package_name: str) -> str:
        if self._pagure_origin:
            return f'/api/0/{package_name}/tree'
        else:
            return f'/api/0/rpms/{package_name}/tree'


class OpenSusePackage(LinuxPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._base_api_url = 'https://api.opensuse.org/'
        self._headers = OpenSusePackage._get_headers()
        self._project_name = ''
        self._vendor = 'OpenSuse'

    async def initialize(self) -> Self:
        self._homepage = await self.get_data_for_universal_package()
        if self._homepage:
            return self
        spec_file_url = await self._get_spec_file_url()
        if not spec_file_url:
            return self
        try:
            response = await self._fetch_text_response(spec_file_url)
        except ResponseError:
            return self
        spec_file_content = response.get_content()
        await async_to_thread(self._semaphore, self._parse_spec_content, spec_file_content)
        return self

    async def _get_spec_file_url(self) -> str:
        package_sources_url = await self._get_package_sources_url()
        if not package_sources_url:
            return ''
        try:
            response = await self._fetch_text_response(package_sources_url)
        except ResponseError:
            return ''
        content = response.get_content()
        root = fromstring(content)
        files = root.findall('./entry')
        if not files:
            return ''
        spec_files = []
        matched_spec_file_name = None
        searched_spec_file_name = f'{self._name}.spec'
        for file in files:
            file_name = file.get('name', '')
            if '.spec' in file_name:
                spec_files.append(file_name)
                if searched_spec_file_name in file_name:
                    matched_spec_file_name = file_name
                    break
        if not spec_files:
            return ''
        if not matched_spec_file_name:
            matched_spec_file_name = spec_files[0]
        spec_file_path = f'/source/{self._project_name}/{self._name}/{matched_spec_file_name}'
        spec_file_url = urljoin(self._base_api_url, spec_file_path)
        return spec_file_url

    async def _get_package_sources_url(self) -> str:
        if not self._raw_name:
            return ''
        path = f"/search/package?match=@name='{self._raw_name}'"
        url = urljoin(self._base_api_url, path)
        try:
            response = await self._fetch_text_response(url)
        except ResponseError:
            return ''
        content = response.get_content()
        root = fromstring(content)
        packages = root.findall('package')
        if not packages:
            return ''
        package, project_name = self._select_package_with_project(packages)
        package_name = package.get('name') or self._raw_name
        if not package_name or not project_name:
            return ''
        self._name = package_name
        self._project_name = project_name
        title_element = package.find('title')
        description_element = package.find('description')
        title = title_element.text if title_element is not None else ''
        description = description_element.text if description_element is not None else ''
        self._description = '\n'.join([title, description]).strip()
        package_sources_path = f'/source/{project_name}/{package_name}'
        package_sources_url = urljoin(self._base_api_url, package_sources_path)
        self._package_url = f'https://software.opensuse.org/package/{self._name}'
        return package_sources_url

    @staticmethod
    def _select_package_with_project(packages: list[Element]) -> tuple[Element, str]:
        index_of_last = len(packages) - 1
        for j in range(index_of_last, -1, -1):
            package = packages[j]
            project_name = package.get('project') or ''
            if 'suse' in project_name.lower():
                return package, project_name
        project_name = packages[0].get('project') or ''
        return packages[-1], project_name

    @staticmethod
    def _get_headers() -> dict[str, str]:
        username = getenv('LINUX_RECOGNITION__OBS_USERNAME')
        password = getenv('LINUX_RECOGNITION__OBS_PASSWORD')
        raw_credentials = f'{username}:{password}'.encode('utf-8')
        credentials = b64encode(raw_credentials).decode('utf-8')
        return {
            'Authorization': f'Basic {credentials}',
            'Accept': 'application/xml; charset=utf-8'
        }

    async def _fetch_text_response(self, url, **kwargs) -> TextResponse:
        custom_parameters = {
            'headers': self._headers
        }
        custom_parameters.update(**kwargs)
        return await super()._fetch_text_response(url, **custom_parameters)


class AlpinePackage(LinuxPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._vendor = 'Alpine Linux'

    async def initialize(self, family: str | None = None) -> Self:
        project_director = self._recognition_context.project_directory
        package_info = await fetch_alpine_package_info(
            self._db_pools.packages, self._raw_name, project_director
        )
        if package_info is None:
            return self
        self._name = fetch(package_info, 'srcname', default=self._raw_name)
        self._description = fetch(package_info, 'description', default='')
        self._license_info = LicenseInfo([fetch(package_info, 'license', output_type=str)])
        if package_info['homepage']:
            self._package_url = self._homepage = package_info['homepage']
        return self


class ArchPackage(LinuxPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._base_url = 'https://archlinux.org/'
        self._query_url = f'{self._base_url}packages/search/json/?q={self._raw_name}'
        self._vendor = 'Arch Linux'

    async def initialize(self)  -> Self:
        self._homepage = await self.get_data_for_universal_package()
        if self._homepage:
            return self
        matching_packages_info = await self._get_matching_results()
        if not matching_packages_info:
            return self

        def is_base_package_info(result) -> bool:
            pkg_name = fetch(result, 'pkgname', output_type=str).strip()
            pkg_base = fetch(result, 'pkgbase', output_type=str).strip()
            return True if pkg_base == pkg_name else False

        package_info = next(
            (result for result in matching_packages_info if is_base_package_info(result)),
            matching_packages_info[0]
        )
        await async_to_thread(self._semaphore, self._fetch_package_details, package_info)
        return self

    async def _get_matching_results(self) -> list[Mapping]:
        try:
            response = await self._fetch_json_response(self._query_url)
        except ResponseError:
            return []
        response_content: Mapping[str, Any] = response.get_content()
        if not response_content or 'results' not in response_content:
            return []
        results = fetch(response_content, 'results', output_type=list)
        exact_results = [result for result in results if self._raw_name in (
            fetch(result, name, output_type=str) for name in ['pkgname', 'pkgbase'])]
        if exact_results:
            return exact_results
        return [result for result in results if '-' in self._raw_name and any(self._raw_name in fetch(
            result, name, output_type=str) for name in ['pkgname', 'pkgbase'])]

    def _fetch_package_details(self, package_info: Mapping) -> None:
        self._name = package_info.get('pkgbase') or package_info.get('pkgname') or ''
        self._name.strip()
        self._description = fetch(package_info, 'pkgdesc', output_type=str).strip()
        self._license_Info = LicenseInfo(fetch(package_info, 'licenses', output_type=list))
        self._homepage = fetch(package_info, 'url', output_type=str).strip()
        self._package_url = self._homepage or f'{self._base_url}packages/?q={self._name}'

class UniversalPackage(LinuxPackage):

    def __init__(
            self,
            recognition_context: RecognitionContext,
            fingerprint: Fingerprint,
            family: str | None = None
    ) -> None:
        super().__init__(recognition_context, fingerprint, family)
        self._raw_publisher = fingerprint.publisher

    async def initialize(self) -> Self:
        raw_name = self._raw_name.lower()
        project_directory = self._recognition_context.project_directory
        package_info = await fetch_package_info(
            raw_name, self._family, self._db_pools.repology, self._is_host_supported, project_directory
        )
        if package_info is None:
            return self
        self._name = package_info['projectname_seed'] or self._raw_name
        self._description = package_info['description'] or ''
        self._license_info = LicenseInfo(fetch(package_info, 'licenses', output_type=list))
        self._homepage = package_info['homepage'] or fetch(package_info, 'project_url', default= '')
        self._package_url = self._homepage or fetch(package_info, 'package_url', default= '')
        return self

    def get_vendor(self) -> str:
        vendors_by_distro = {
            'debian': "Debian",
            'ubuntu': 'Ubuntu',
            'fedora': 'Fedora',
            'red hat': 'Red Hat',
            'opensuse': 'openSUSE',
            'suse': 'SUSE',
            'alpine': 'Alpine Linux',
            'amazon': 'Amazon',
            'rocky': 'Rocky Enterprise Software Foundation',
            'alma': 'AlmaLinux',
            'centos': 'CentOS'
        }
        raw_publisher = self._raw_publisher.lower()
        for distro in vendors_by_distro:
            if distro in raw_publisher:
                self._vendor = vendors_by_distro[distro]
                break
        return self._vendor


def get_package_tools(distro: str) -> PackageTools | None:
    package_tools = _get_package_tools()
    if distro not in package_tools:
        return None
    return PackageTools(
        **package_tools[distro]
    )


def get_supported_distros() -> list[str]:
    package_tools = _get_package_tools()
    return list(package_tools.keys())


async def correct_url(recognition_context: RecognitionContext, url: str, package_name: str) -> str:
    incomplete_github_pattern = r'github\.com/([^/]+)/*$'
    incomplete_github_match = re.search(incomplete_github_pattern, url)
    if incomplete_github_match is not None:
        username = incomplete_github_match.group(1)
        return await _find_full_github_url(recognition_context, url, username, package_name)
    excessive_github_pattern = r'github(\.com)/[^/]+/[^/]+/[^/]+'
    excessive_github_match = re.search(excessive_github_pattern, url)
    if excessive_github_match is not None:
        return await _correct_github_url(recognition_context, url, package_name)
    url_hostname = str(urlparse(url).hostname)
    if 'metacpan.org' in url_hostname:
        return _correct_metacpan_url(recognition_context, url, package_name)
    if 'sourceforge' in url_hostname:
        return _correct_sourceforge_url(url)
    return url


async def _correct_github_url(
        recognition_context: RecognitionContext,
        url: str,
        package_name: str
) -> str:
    repo_url_pattern = r'github\.com/([^/]+)/([^/]+)'
    repo_url_match = re.search(repo_url_pattern, url)
    if not repo_url_match:
        return url
    username = repo_url_match.group(1)
    repo_name = repo_url_match.group(2)
    repo_path = f'/{username}/{repo_name}'
    github_webpage_match = re.search(r'\.', repo_name)
    if github_webpage_match is None:
        return f'https://github.com{repo_path}'
    return await _find_full_github_url(recognition_context, url, username, package_name)


async def _find_full_github_url(
        recognition_context: RecognitionContext,
        url: str,
        username: str,
        package_name: str
) -> str:
    user_repos_url = f'https://api.github.com/users/{username}/repos'
    params = {'per_page': 100}
    response = JsonResponse(
        user_repos_url,
        session_manager=recognition_context.session_handler,
        session_name='github',
        params = params,
        semaphore=recognition_context.synchronization.semaphore
    )
    try:
        response = await response.fetch()
    except ResponseError:
        return url
    parts_separator = re.compile(r'[-_\s]+')
    package_name_parts = parts_separator.split(package_name)
    package_parts = [p.lower() for p in package_name_parts]

    def get_relevance(repo: Mapping) -> dict[str, Any] | None:
        repo_name = fetch(repo, 'name', output_type=str).strip()
        if not repo_name:
            return None
        repo_name_parts = parts_separator.split(repo_name)
        repo_parts = [p.lower() for p in repo_name_parts]
        if all(p in repo_parts for p in package_parts) or all(p in package_parts for p in repo_parts):
            html_url = fetch(repo, 'html_url', output_type=str).strip()
            if html_url:
                return {'parts': repo_parts, 'url': html_url}
        return None

    response_content = response.get_content()
    repos_relevance = [get_relevance(repo) for repo in response_content]
    relevant = [repo for repo in repos_relevance if repo is not None]
    if not relevant:
        return url
    return next((r['url'] for r in relevant if r['parts'] == package_parts), relevant[0])


def _correct_metacpan_url(recognition_context: RecognitionContext, url: str, package_name: str, ) -> str:
    url = url.rstrip('/')
    if url.startswith('https://fastapi.metacpan.org/v1/release/'):
        return url
    path = urlparse(url).path
    if not path[1:]:
        if not package_name:
            return url
        perl_pattern = recognition_context.library_patterns.perl
        perl_match = perl_pattern.search(package_name)
        if perl_match is not None:
            group_dict = perl_match.groupdict()
            name = (
                group_dict['debian'].title() if group_dict['debian'] is not None else group_dict['fedora']
            ).strip()
        else:
            name = package_name
    else:
        name = url.rsplit('/', 1)[-1]
    distribution_name = re.sub(r'-\d+\.\d+', '', name).replace('::','-').strip()
    release_api_url = f'https://fastapi.metacpan.org/v1/release/{distribution_name}'
    return release_api_url


def _correct_sourceforge_url(url: str) -> str:
    sourceforge_pattern = r'sourceforge\.net/projects/[^/]+'
    sourceforge_match = re.search(sourceforge_pattern, url)
    return url[:sourceforge_match.end()] if sourceforge_match else url


def _get_atomic_license_items(license_condition: str) -> Iterable:
    if not license_condition:
        return []
    pattern = r'(?:and|^)\s*(\()?(?P<license>(?:(?!\s*and).)*)(?(1)\))'
    return re.finditer(pattern, license_condition, flags=re.IGNORECASE)


def _get_package_tools() -> dict[str, dict[str, Any]]:
    package_tools_by_distro = {
        'debian': {'classes': [DebianPackage], 'family': 'debuntu'},
        'ubuntu': {'classes': [UbuntuPackage], 'family': 'debuntu'},
        'fedora': {'classes': [FedoraPackage], 'family': 'fedora'},
        'red hat': {'classes': [FedoraPackage], 'family': 'fedora'},
        'opensuse': {'classes': [OpenSusePackage], 'family': 'opensuse'},
        'suse': {'classes': [OpenSusePackage], 'family': 'opensuse'},
        'alpine': {'classes': [AlpinePackage], 'family': 'alpine'},
        'amazon': {'classes': [FedoraPackage], 'family': 'fedora'},
        'rocky': {'classes': [UniversalPackage], 'family': 'centos'},
        'alma': {'classes': [UniversalPackage], 'family': 'centos'},
        'centos': {'classes': [UniversalPackage], 'family': 'centos'},
        'arch': {'classes': [ArchPackage], 'family': 'arch'},
    }
    return package_tools_by_distro
