import re
from asyncio import Semaphore
from logging import getLogger, DEBUG
from typing import Self

from jinja2 import Environment

from linux_recognition.context import RecognitionContext
from linux_recognition.data.predefined import PREDEFINED_PROPERTIES
from linux_recognition.db.postgresql.cpe import get_cpe_entities
from linux_recognition.db.postgresql.licenses import fetch_licenses
from log_management import get_error_details
from linux_recognition.normalization import Fingerprint
from linux_recognition.reposcan.packages import (
    correct_url,
    get_package_tools,
    UniversalPackage,
    LinuxPackage,
    get_supported_distros
)
from linux_recognition.reposcan.projects import MetaCPANProject, PyPIProject, RubyGemProject, url_to_project
from linux_recognition.reposcan.projects_base import Project
from linux_recognition.synchronization import async_to_thread
from linux_recognition.typestore.datatypes import Brand, LicenseInfo, LlmInteraction, PackageTools, SessionHandler
from linux_recognition.typestore.errors import LinuxRecognitionError
from linux_recognition.webtools.response import fetch_html_text


logger = getLogger(__name__)


class SoftwareRecognizer:

    def __init__(self, fingerprint: Fingerprint, recognition_context: RecognitionContext) -> None:
        self.software: Brand | None = None
        self.publisher: Brand | None = None
        self.version: str | None =  None
        self.homepage: str | None = None
        self.description: str = ''
        self.unspsc: str = ''
        self.licenses: list[str] = []
        self.release_date: str = ''
        self.cpe_string: str = ''
        self._recognition_context: RecognitionContext = recognition_context
        self._jinja_environment: Environment = recognition_context.jinja_environment
        self._llm_interaction: LlmInteraction = self._recognition_context.llm_interaction
        self._semaphore: Semaphore = self._recognition_context.synchronization.semaphore
        self._fingerprint: Fingerprint = fingerprint
        self._raw_software: str = self._fingerprint.software
        self._raw_publisher: str = self._fingerprint.publisher
        self._raw_version: str = self._fingerprint.version
        self._version_with_suffix: str
        if fingerprint.version_suffix is not None:
            self._version_with_suffix = f'{self._raw_version} {fingerprint.version_suffix}'
        else:
            self._version_with_suffix = self._raw_version
        self._package_tools: PackageTools | None = None
        self._package_instance: LinuxPackage | None = None
        self._utilized_package_class_name: str | None = None
        self._package_name: str | None = None
        self._package_vendor: str | None = None
        self._package_url: str | None = None
        self._description: str = ''
        self._license_info: LicenseInfo = LicenseInfo([])
        self._homepage_candidate: str | None = None
        self._fetch_method_used: str | None = None
        self._matching_cpe_entities: list[tuple[str, str, str]] = []

    async def recognize(self) -> Self | None:
        try:
            await self._recognize()
        except LinuxRecognitionError:
            return None
        except Exception as e:
            self._log_attributes()
            extra = get_error_details(e)
            logger.error('Unexpected error', exc_info=logger.isEnabledFor(DEBUG), extra=extra)
            return None
        self._log_attributes()
        return self if self.software is not None else None

    async def _recognize(self) -> None:
        if not self._raw_software:
            return
        await self._recognize_library_package()
        if self.software is not None:
            self._fetch_method_used = '_recognize_library_package'
            return
        await self._collect_core_info()
        if not self._package_url:
            return
        await self._retrieve_package_information()

    async def _recognize_library_package(self) -> None:
        await self._recognize_python_package()
        if self.software is not None:
            return
        ruby_pattern = self._recognition_context.library_patterns.ruby
        ruby_match = ruby_pattern.search(self._raw_software)
        if ruby_match is not None:
            await self._recognize_ruby_package(ruby_match)
            return
        perl_pattern = self._recognition_context.library_patterns.perl
        perl_match = perl_pattern.search(self._raw_software)
        if perl_match is not None:
            await self._recognize_perl_package(perl_match)

    async def _recognize_python_package(self) -> None:
        pattern = self._recognition_context.library_patterns.python
        python_match = pattern.search(self._raw_software)
        if python_match is None:
            return
        group_dict = python_match.groupdict()
        subpackages = ['debug', 'devel', 'idle', 'libs', 'python', 'stdlib', 'test', 'tkinter']
        package = group_dict['package']
        version = group_dict['version']
        version_formatted = version if len(version) <= 1 else f'{version[0]}.{version[1:]}'
        if package.lower() in subpackages and self._raw_version.startswith(version_formatted):
            self._package_name = 'python'
            self._fetch_predefined(self._package_name)
            return
        self._package_name = package
        pypi_project_url = f'https://pypi.org/project/{self._package_name}'
        await self._scan_public_project(pypi_project_url, project_class=PyPIProject)

    async def _recognize_ruby_package(self, ruby_match: re.Match) -> None:
        self._package_name = ruby_match.group(1)
        ruby_gem_api_url = f'https://rubygems.org/api/v1/search.json?query={self._package_name}'
        await self._scan_public_project(ruby_gem_api_url, project_class=RubyGemProject)

    async def _recognize_perl_package(self, perl_match: re.Match) -> None:
        group_dict = perl_match.groupdict()
        if group_dict['debian'] is not None:
            package_name = group_dict['debian'].title()
        else:
            package_name = group_dict['fedora']
        parts_pattern = r'([^:\s_-]+)'
        parts = re.findall(parts_pattern, package_name)
        if not parts:
            return
        case_sensitive = parts[0][0].isupper()
        if case_sensitive:
            for index, part in enumerate(parts[1:]):
                if not part[0].isupper():
                    parts = parts[:index + 1]
                    break
        self._package_name = '-'.join(parts)
        release_api_url = f'https://fastapi.metacpan.org/v1/release/{self._package_name}'
        await self._scan_public_project(release_api_url, project_class=MetaCPANProject)

    def _fetch_predefined(self, package_name: str) -> None:
        self.software = Brand(**PREDEFINED_PROPERTIES[package_name]['software'])
        self.publisher = Brand(**PREDEFINED_PROPERTIES[package_name]['publisher'])
        self.version = self._version_with_suffix
        self.homepage = PREDEFINED_PROPERTIES[package_name]['homepage']
        self.description = PREDEFINED_PROPERTIES[package_name]['description']
        self.unspsc = PREDEFINED_PROPERTIES[package_name]['unspsc']
        self.licenses = PREDEFINED_PROPERTIES[package_name]['licenses']
        self.cpe_string = PREDEFINED_PROPERTIES[package_name]['cpe_string']

    async def _collect_core_info(self) -> None:
        supported_distros = get_supported_distros()
        for distro in supported_distros:
            if distro in self._raw_publisher.lower():
                self._package_tools = get_package_tools(distro)
                break
        await self._fetch_with_corresponding_classes()
        if not self._package_url:
            await self._fetch_with_universal_class()

    async def _fetch_with_corresponding_classes(self) -> None:
        if self._package_tools is None:
            return
        package_classes = self._package_tools.classes
        for cls in package_classes:
            cls: type[LinuxPackage]
            package_instance = cls(
                self._recognition_context, self._fingerprint, family=self._package_tools.family
            )
            await package_instance.initialize()
            self._package_url = package_instance.get_package_url()
            if not self._package_url:
                continue
            self._utilized_package_class_name = cls.__name__
            self._fetch_metadata_for_recognized_package(package_instance)
            return

    async def _fetch_with_universal_class(self) -> None:
        package_instance = UniversalPackage(
            self._recognition_context,
            self._fingerprint
        )
        await package_instance.initialize()
        self._package_url = package_instance.get_package_url()
        if not self._package_url:
            return
        self._utilized_package_class_name = UniversalPackage.__name__
        self._fetch_metadata_for_recognized_package(package_instance)

    def _fetch_metadata_for_recognized_package(self, package_instance: LinuxPackage):
        self._package_instance = package_instance
        self._package_name = package_instance.get_name() or self._raw_software
        self._package_vendor = package_instance.get_vendor()
        self._homepage_candidate = package_instance.get_homepage()
        self._description = package_instance.get_description()
        self._license_info = package_instance.get_license_info()

    async def _retrieve_package_information(self) -> None:
        if not self._homepage_candidate:
            await self._recognize_distro_specific_package()
            return
        self._homepage_candidate = await correct_url(
            self._recognition_context, self._homepage_candidate, self._package_name
        )
        project_class = url_to_project(self._homepage_candidate)
        if project_class is not None:
            await self._scan_public_project(self._homepage_candidate, project_class=project_class)
        else:
            await self._fetch_for_external()

    async def _recognize_distro_specific_package(self) -> None:
        self._fetch_method_used = '_recognize_distro_specific_package'
        self.software = Brand(self._package_name)
        self.publisher = Brand(self._package_vendor)
        self.version = self._version_with_suffix
        if self._fingerprint.version_is_date:
            self.release_date = self._fingerprint.date_in_version.iso_format()
        self._homepage_candidate = self._package_url
        await self._resolve_description_and_homepage()
        await self._resolve_software_properties()

    async def _scan_public_project(self, url: str, project_class: type(Project)) -> None:
        self._fetch_method_used = '_scan_public_project'
        project: Project = project_class(
            self._recognition_context,
            self._fingerprint,
            url=url,
            package_instance=self._package_instance
        )
        await project.initialize()
        if not project.is_membership_confirmed():
            return
        self.software: Brand = await project.get_software()
        self.publisher: Brand = await project.get_publisher()
        release = await project.get_release()
        self.version = release.version or self._version_with_suffix
        self.release_date = release.date
        self.homepage = project.get_homepage()
        self._description = await project.get_description()
        self._license_info = await project.get_license_info()
        await self._resolve_software_properties()

    async def _fetch_for_external(self) -> None:
        self._fetch_method_used = '_fetch_for_external'
        self.software = Brand(self._package_name)
        self.publisher = Brand(self._package_name)
        self.version = self._version_with_suffix
        if self._fingerprint.version_is_date:
            self.release_date = self._fingerprint.date_in_version.iso_format()
        await self._resolve_description_and_homepage()
        await self._resolve_software_properties()

    async def _resolve_description_and_homepage(self):
        if not self._description:
            session_manager: SessionHandler = self._recognition_context.session_handler
            html_parse = await fetch_html_text(self._homepage_candidate, session_manager)
            parsed = html_parse.parsed
            response_url = html_parse.url
            if self._homepage_candidate == response_url or self._is_webpage_related(
                    parsed.lower(), response_url
            ):
                self.homepage = response_url
                self._description = parsed
            else:
                self.homepage = self._homepage_candidate
        else:
            self.homepage = self._homepage_candidate

    def _is_webpage_related(self, content: str, url: str) -> bool:
        name_parts = re.split(r'[-\s]+', self._package_name)
        name_parts_lower = [part.lower() for part in name_parts]
        relevant_parts = [part for part in name_parts_lower if len(part) >= 3]
        if not relevant_parts:
            if all(part in url for part in name_parts_lower):
                return True
            if all(part in content.lower() for part in name_parts_lower):
                return True
            return False
        if any(part in url for part in relevant_parts):
            return True
        if any(part in content.lower() for part in relevant_parts):
            return True
        return False

    async def _resolve_software_properties(self) -> None:
        await self._resolve_cpe_info()
        if self._description:
            self.description = await self._llm_interaction.generate_formal_definition(
                self._description, self.software.name
            )
        if self._license_info.content:
            await self._process_license_resolution()

    async def _resolve_cpe_info(self) -> None:
        software_names = [
            name.lower().replace(' ', '_') for name in [
                self.software.name, *self.software.alternative_names
            ]
        ]
        self._matching_cpe_entities = await get_cpe_entities(
            self._recognition_context.source_db_pools.packages,
            self._jinja_environment,
            software_names,
            self._semaphore
        )
        await async_to_thread(
            self._semaphore, self._establish_cpe_identity, self._matching_cpe_entities
        )

    def _establish_cpe_identity(self, records: list[tuple[str, str, str]]) -> None:
        publisher_names = [name for name in [self.publisher.name, *self.publisher.alternative_names] if name]
        publisher_names_lower = {name.lower() for name in publisher_names}
        version_parts_pattern = re.compile(r'[^\W_]+')
        version_parts = version_parts_pattern.findall(self.version)
        version_parts_count = len(version_parts)
        matched_cpe_publisher, matched_cpe_product = None, None
        for cpe_publisher, cpe_product, cpe_version in records:
            cpe_publisher_readable = cpe_publisher.replace('_', ' ')
            if cpe_publisher_readable in publisher_names_lower:
                matched_cpe_publisher, matched_cpe_product = cpe_publisher, cpe_product
                break
            cpe_version_parts = version_parts_pattern.findall(cpe_version)
            if not version_parts or not cpe_version_parts:
                continue
            if version_parts == cpe_version_parts[:version_parts_count]:
                matched_cpe_publisher, matched_cpe_product = cpe_publisher, cpe_product
                break
        if matched_cpe_publisher is None:
            return
        cpe_publisher_stripped_end = matched_cpe_publisher.rsplit(
            '_', 1)[0].replace('_', ' ')
        cpe_publisher_readable = matched_cpe_publisher.replace('_', ' ')
        publisher_alternative_names = [
            name for name in publisher_names
            if cpe_publisher_stripped_end in name.lower() and name.lower() != cpe_publisher_readable
        ]
        self.publisher = Brand(cpe_publisher_readable.title(), publisher_alternative_names)
        self.cpe_string = f'cpe:2.3:a:{matched_cpe_publisher}:{matched_cpe_product}:'

    async def _process_license_resolution(self):
        if self._license_info.is_raw_text:
            license_identifiers = await self._llm_interaction.extract_licenses(
                self._license_info.content[0], self.software.name
            )
        else:
            license_identifiers = self._extract_license_identifiers()
        pool = self._recognition_context.source_db_pools.packages
        matched_items = await fetch_licenses(pool, self._jinja_environment, license_identifiers, self._semaphore)
        matched_identifiers = [item.identifier for item in matched_items]
        licenses = [item.name for item in matched_items]
        unmatched_identifiers = [
            identifier for identifier in license_identifiers if identifier not in matched_identifiers
        ]
        if unmatched_identifiers:
            resolved_licenses = await self._recognition_context.license_resolver.resolve(
                unmatched_identifiers
            )
            licenses.extend(resolved_licenses)
        self.licenses = licenses

    def _extract_license_identifiers(self) -> list[str]:
        separator = re.compile(r'[()\s]+(?:or|and)[()\s]+', re.IGNORECASE)
        license_identifiers = []
        for item in self._license_info.content:
            identifiers = separator.split(item.strip(' ()'))
            license_identifiers.extend(identifiers)
        return license_identifiers

    def _log_attributes(self) -> None:
        attributes_to_log = [
            '_fingerprint',
            '_utilized_package_class_name',
            '_package_name',
            '_package_vendor',
            "_package_url",
            '_description',
            '_license_info',
            '_homepage_candidate',
            '_fetch_method_used',
            '_matching_cpe_entities',
        ]
        extra = {}
        for attribute in attributes_to_log:
            extra[attribute] = getattr(self, attribute)
        extra['_package_tools'] = self._package_tools.dict_repr if self._package_tools is not None else None
        logger.info('Attributes log', extra=extra)
