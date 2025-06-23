import re

from linux_recognition.reposcan.dateparse import parse_date_from_digits
from linux_recognition.typestore.datatypes import Fingerprint, FingerprintDict, VersionNormalizationPatterns


class FingerprintNormalizer:

    def __init__(self, fingerprint: FingerprintDict, patterns: VersionNormalizationPatterns) -> None:
        self._software = (fingerprint['software'] or '').strip()
        self._publisher = (fingerprint['publisher'] or '').strip()
        self._version = (fingerprint['version'] or '').strip()
        self._version_uncut = self._version
        self._suffix_pattern = patterns.suffix
        self._date_pattern = patterns.date
        self._separator_pattern = patterns.separator
        self._software_pre_hyphen = ''
        self._version_uncut = ''
        self._date_in_version = None
        self._version_is_date = False
        self._version_suffix = None

    def get_normalized(self) -> Fingerprint:
        self._normalize()
        return Fingerprint(
            software=self._software,
            publisher=self._publisher,
            version=self._version,
            _version_uncut=self._version_uncut,
            _date_in_version=self._date_in_version,
            _version_is_date=self._version_is_date,
            _version_suffix=self._version_suffix
        )

    def _normalize(self) -> None:
        self._normalize_software()
        self._normalize_version()

    def _normalize_software(self) -> None:
        self._software = self._software.rsplit(':', 1)[0]
        architecture_pattern = r'(?<!:):[^:]+$'
        self._software = re.sub(architecture_pattern, '', self._software).strip()

    def _normalize_version(self) -> None:
        if not self._version:
            return
        hyphen_chunks = self._version.rsplit('-', 1)
        version = hyphen_chunks[0] if len(hyphen_chunks[0].strip()) > 1 else hyphen_chunks[-1]
        version = version.lstrip('+ ~')
        version = version.rsplit('~', 1)[0]
        version = version.rsplit('+', 1)[0]
        version = version.rstrip(': ')
        version = version.split(':', 1)[-1]
        repo_pattern = r'\W*(?:git|svn|\bfc\d|\bel\d|debian|ubuntu)'
        repo_match = re.search(repo_pattern, version)
        if repo_match is not None:
            version = version[:repo_match.start()] or version[repo_match.end():]
        self._version = version.strip()
        self._version_uncut = self._version
        self._extract_suffix_from_version()
        self._extract_date_from_version()
        if self._version_is_date:
            return
        self._find_optimal_format_for_version()

    def _extract_date_from_version(self) -> None:
        digits_match = self._date_pattern.search(self._version_uncut)
        if digits_match is None:
            return
        date_in_version = parse_date_from_digits(digits_match)
        if date_in_version is None:
            return
        self._date_in_version = date_in_version
        self._version_is_date = digits_match.group() == self._version

    def _extract_suffix_from_version(self) -> None:
        suffix_match = self._suffix_pattern.search(self._version_uncut)
        if suffix_match is None:
            return
        self._version_suffix = suffix_match.group(1)
        suffix_start = self._version.find(self._version_suffix)
        self._version = self._version[:suffix_start]

    def _find_optimal_format_for_version(self) -> None:
        if not self._version:
            return
        components = self._separator_pattern.split(self._version)
        components = components if components[-1] else components[:-2]
        if len(components) // 2 < 2:
            self._version = ''.join(components)
        else:
            self._version = ''.join(components[:-2])


def normalize_fingerprints(
        fingerprints: list[FingerprintDict],
        patterns: VersionNormalizationPatterns
) -> list[Fingerprint]:
    return list(set([FingerprintNormalizer(fp, patterns).get_normalized() for fp in fingerprints]))
