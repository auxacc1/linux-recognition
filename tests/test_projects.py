from typing import Any

import pytest

from typestore.datatypes import VersionNormalizationPatterns
from normalization import FingerprintNormalizer
from reposcan.projects_base import Project
from reposcan.projects import get_supported_projects
from typestore.datatypes import RecognitionContext, Brand, Release


input_samples = {
    'GitHubProject': {
        'url': 'https://github.com/malthe/chameleon',
        'fingerprint': {
            'software': 'python3-chamelon',
            'publisher': 'Fedora Project',
            'version': '4.4.0'
        }
    },
    'GitLabProject': {
        'url': 'https://gitlab.com/dokos/dodock',
        'fingerprint': {
            'software': 'Dodock',
            'publisher': 'Fedora Project',
            'version': '4.51.0'
        }
    },
    'MetaCPANProject': {
        'url': 'https://fastapi.metacpan.org/v1/release/Class-Singleton',
        'fingerprint': {
            'software': 'perl-Class-Singleton',
            'publisher': 'Red Hat',
            'version': '1.5-9.el8'
        }
    },
    'PagureProject': {
        'url': 'https://pagure.io/mailcap/',
        'fingerprint': {
            'software': 'mailcap',
            'publisher': 'Fedora Project',
            'version': '2.1.54'
        }
    },
    'PyPIProject': {
        'url': 'https://pypi.org/project/defusedxml/',
        'fingerprint': {
            'software': 'python3-defusedxml',
            'publisher': 'Fedora Project',
            'version': '0.7'
        }
    },
    'RubyGemProject': {
        'url': 'https://rubygems.org/api/v1/search.json?query=pg',
        'fingerprint': {
            'software': 'rubygem-pg',
            'publisher': 'Red Hat, Inc.',
            'version': '1.5.3'
        }
    },
    'SourceForgeProject': {
        'url': 'https://sourceforge.net/projects/soprano',
        'fingerprint': {
            'software': 'soprano',
            'publisher': 'Red Hat, Inc.',
            'version': '2.9.2'
        }
    }
}


output = {
    'GitHubProject': {
        'software': Brand(name='chameleon', alternative_names=[]),
        'publisher': Brand(name='Malthe Borch', alternative_names=['malthe']),
        'homepage': 'https://chameleon.readthedocs.io',
        'licenses': [
            {"b'The majority of'",}
        ],
        'release': Release(version='4.4', date='2023-12-12')
    },
    'GitLabProject': {
        'software': Brand(name='Dodock', alternative_names=[]),
        'publisher': Brand(name='Dokos', alternative_names=[]),
        'homepage': 'https://gitlab.com/dokos/dodock',
        'licenses': [
            {'MIT License',}
        ],
        'release': Release(version='4.51', date='2025-02-04')
    },
    'MetaCPANProject': {
        'software': Brand(
            name='Class-Singleton',
            alternative_names=['libclass-singleton-perl', 'Class::Singleton', 'perl-Class-Singleton']
        ),
        'publisher': Brand(
            name='Andy Wardley, Steve Hay',
            alternative_names=['SHAY']
        ),
        'homepage': 'https://metacpan.org/dist/Class-Singleton',
        'licenses': [
            {'Artistic License 1.0', 'GNU General Public License v1.0 only',}
        ],
        'release': Release(version='1.5', date='2014-11-07')
    },
    'PagureProject': {
        'software': Brand(name='Mailcap', alternative_names=[]),
        'publisher': Brand(name='Red Hat', alternative_names=[]),
        'homepage': 'https://pagure.io/mailcap',
        'licenses': [
            {'Public Domain and MIT',
             'LicenseRef-Fedora-Public-Domain AND MIT',
             'LicenseRef-Fedora-Public-Domain AND MIT AND metamail'},
            {'Public Domain and MIT',
             'LicenseRef-Fedora-Public-Domain AND MIT',
             'LicenseRef-Fedora-Public-Domain AND MIT AND metamail'}
        ],
        'release': Release(version='2.1', date='')
    },
    'PyPIProject': {
        'software': Brand(name='defusedxml', alternative_names=[]),
        'publisher': Brand(name='Christian Heimes', alternative_names=['Tiran']),
        'homepage': 'https://github.com/tiran/defusedxml',
        'licenses': [
            {'PSFL',}
        ],
        'release': Release(version='0.7', date='2021-03-04')
    },
    'RubyGemProject': {
        'software': Brand(name='Pg', alternative_names=['rubygem-pg']),
        'publisher': Brand(name='Michael Granger', alternative_names=['ged']),
        'homepage': 'https://github.com/ged/ruby-pg',
        'licenses': [
            {'BSD-2-Clause',}
        ],
        'release': Release(version='1.5', date='2023-04-24')
    },
    'SourceForgeProject': {
        'software': Brand(name='Soprano', alternative_names=[]),
        'publisher': Brand(name='Daniele Galdi, Sebastian Trueg, Vishesh Handa', alternative_names=[]),
        'homepage': 'https://sourceforge.net/projects/soprano',
        'licenses': [
            {'GNU Library or Lesser General Public License version 2.0 (LGPLv2)',}
        ],
        'release': Release(version='2.9', date='2013-01-02')
    }
}


@pytest.fixture(scope='class')
def project_classes() -> dict[str, type[Project]]:
    supported_projects = get_supported_projects()
    return {
        r.__name__: r for r in supported_projects
    }


@pytest.fixture(scope='class', params=list(input_samples.keys()))
def samples(request, project_classes) -> dict[str, Any]:
    init: dict[str, Any] = input_samples[request.param]
    init['class'] = project_classes[request.param]
    data = {'init': init, 'output': output[request.param]}
    return data


class TestProject:

    @pytest.fixture(scope='class')
    async def project_instance(self, recognition_context: RecognitionContext, samples: dict) -> Project:
        init_data = samples['init']
        url = init_data['url']
        patterns = VersionNormalizationPatterns()
        fingerprint = FingerprintNormalizer(
            init_data['fingerprint'], patterns=patterns
        ).get_normalized()
        project_class = init_data['class']
        project = project_class(
            recognition_context=recognition_context, fingerprint=fingerprint, url=url
        )
        assert isinstance(project, project_class)
        await project.initialize()
        return project

    async def test_get_software(self, project_instance: Project, samples: dict) -> None:
        expected_output = samples['output']
        brand = await project_instance.get_software()
        assert isinstance(brand, Brand)
        expected_brand: Brand = expected_output['software']
        assert brand.name == expected_brand.name
        assert set(brand.alternative_names) == set(expected_brand.alternative_names)

    async def test_get_publisher(self, project_instance: Project, samples: dict):
        expected_output = samples['output']
        brand = await project_instance.get_publisher()
        assert isinstance(brand, Brand)
        expected_brand: Brand = expected_output['publisher']
        assert brand.name == expected_brand.name
        assert set(brand.alternative_names) == set(expected_brand.alternative_names)

    def test_get_homepage(self, project_instance: Project, samples: dict) -> None:
        expected_output = samples['output']
        homepage = project_instance.get_homepage()
        assert isinstance(homepage, str)
        assert homepage == expected_output['homepage']

    async def test_get_description(self, project_instance: Project) -> None:
        description = await project_instance.get_description()
        assert isinstance(description, str)
        assert description

    async def test_get_licenses(self, project_instance: Project, samples: dict) -> None:
        expected_output = samples['output']
        license_info = await project_instance.get_license_info()
        content = license_info.content
        assert isinstance(content, list)
        assert set([l[:16] for l in content]) in ({l[:16] for l in item} for item in expected_output['licenses'])

    async def test_get_release(self, project_instance: Project, samples: dict) -> None:
        expected_output = samples['output']
        release = await project_instance.get_release()
        assert isinstance(release, Release)
        assert release == expected_output['release']
