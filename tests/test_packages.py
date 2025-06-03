import pytest

from reposcan.packages import (
    DebianPackage,
    FedoraPackage,
    LinuxPackage,
    UbuntuPackage,
    AlpinePackage,
    ArchPackage,
    UniversalPackage
)
from typestore.datatypes import Fingerprint, LicenseInfo, RecognitionContext


input_samples = {
    'DebianPackage': {
        'class': DebianPackage,
        'fingerprint': Fingerprint(
            software='libprotobuf-dev',
            publisher='Debian',
            version='3.21'
        ),
        'family': 'debuntu'
    },
    'UbuntuPackage': {
        'class': UbuntuPackage,
        'fingerprint': Fingerprint(
            software='ubuntu-desktop-minimal',
            publisher='Ubuntu',
            version='1.481.1'
        ),
        'family': 'debuntu'
    },
    'FedoraPackage': {
        'class': FedoraPackage,
        'fingerprint': Fingerprint(
            software='python2-chameleon',
            publisher='Fedora',
            version='2.9'
        ),
        'family': 'fedora'
    },
    'AlpinePackage': {
        'class': AlpinePackage,
        'fingerprint': Fingerprint(
            software='alpine-repo-tools',
            publisher='Alpine',
            version='0.3.1'
        ),
        'family': 'alpine'
    },
    'ArchPackage': {
        'class': ArchPackage,
        'fingerprint': Fingerprint(
            software='arch-install-scripts',
            publisher='Arch',
            version='29'
        ),
        'family': 'arch'
    },
    'UniversalPackage': {
        'class': UniversalPackage,
        'fingerprint': Fingerprint(
            software='protobuf',
            publisher='CentOS',
            version='3.19'
        ),
        'family': None,
    }
}


output = {
    'DebianPackage': {
        'homepage': ['https://github.com/google/protobuf/'],
        'package_url': ['https://github.com/google/protobuf/'],
        'name': ['protobuf'],
        'vendor': ['Debian'],
        'description': [''],
        'licenses': [set()]
    },
    'UbuntuPackage': {
        'homepage': [''],
        'package_url': ['https://launchpad.net/ubuntu/+source/ubuntu-meta'],
        'name': ['ubuntu-meta'],
        'vendor': ['Canonical'],
        'description': [''],
        'licenses': [set(),]
    },
    'FedoraPackage': {
        'homepage': ['https://github.com/malthe/chameleon'],
        'package_url': ['https://github.com/malthe/chameleon'],
        'name': ['Chameleon'],
        'vendor': ['Fedora'],
        'description': [
            (
                'XML-based template compiler\n'
                'Chameleon is an XML attribute language template compiler. It comes with '
                'implementations for the Zope Page Templates (ZPT) and Genshi templating languages. \r\n'
                'The engine compiles templates into Python byte-code. This results in '
                'performance which is on average 10-15 times better than implementations '
                'which use run-time interpretation.'
            )
        ],
        'licenses': [
            {'BSD',},
        ]
    },
    'AlpinePackage': {
        'homepage': ['https://gitlab.alpinelinux.org/alpine/infra/repo-tools'],
        'package_url': ['https://gitlab.alpinelinux.org/alpine/infra/repo-tools'],
        'name': ['alpine-repo-tools'],
        'vendor': ['Alpine Linux'],
        'description': [
            'alpine-repo-tools - utilities to interact with Alpine Linux repositories, '
            'alpine-repo-tools-bash-completion - Bash completions for alpine-repo-tools, '
            'alpine-repo-tools-doc - utilities to interact with Alpine Linux repositories (documentation), '
            'alpine-repo-tools-fish-completion - Fish completions for alpine-repo-tools, '
            'alpine-repo-tools-zsh-completion - Zsh completions for alpine-repo-tools'
        ],
        'licenses': [
            {'MIT',},
        ]
    },
    'ArchPackage': {
        'homepage': [
            'https://gitlab.archlinux.org/archlinux/arch-install-scripts',
        ],
        'package_url': [
            'https://gitlab.archlinux.org/archlinux/arch-install-scripts',
        ],
        'name': ['arch-install-scripts'],
        'vendor': ['Arch Linux'],
        'description': [
            'Scripts to aid in installing Arch Linux',
            ('Bash completions for arch-install-scripts, Scripts to aid in installing Arch Linux, '
            'Scripts to aid in installing Arch Linux (documentation), '
            'Zsh compltions for arch-install-scripts')
        ],
        'licenses': [
            {'GPL-2.0-only',}, {'GPL', 'GPL-2.0-only',},
        ]
    },
    'UniversalPackage': {
        'homepage': ['https://github.com/google/protobuf'],
        'package_url': ['https://github.com/google/protobuf'],
        'name': ['protobuf'],
        'vendor': ['CentOS'],
        'description': [
            "Library for extensible, efficient structure packing, Library for extensible, "
            "efficient structure packing (debug symbols), Library for extensible, efficient structure "
            "packing (development files), Python bindings to Google's data interchange format, Ruby "
            "bindings to Google's data interchange format, Vim syntax for protobuf"
        ],
        'licenses': [
            {'BSD-3-Clause',},
        ]
    }
}


@pytest.fixture(scope='class', params=list(input_samples.keys()))
async def samples(request) -> dict[str, dict]:
    init = input_samples[request.param]
    data = {'init': init, 'output': output[request.param]}
    return data


class TestPackage:

    @pytest.fixture(scope='class')
    async def package(self, recognition_context: RecognitionContext, samples: dict):
        init_data = samples['init']
        package_class: type[LinuxPackage] = init_data['class']
        package_instance = package_class(
            recognition_context,
            init_data['fingerprint'],
            family=init_data['family']
        )
        assert isinstance(package_instance, package_class)
        await package_instance.initialize()
        return package_instance

    def test_get_homepage(self, package: LinuxPackage, samples: dict):
        expected_output = samples['output']
        homepage = package.get_homepage()
        assert isinstance(homepage, str)
        assert homepage in expected_output['homepage']

    def test_get_source_package_url(self, package: LinuxPackage, samples: dict):
        expected_output = samples['output']
        source_package_url = package.get_package_url()
        assert isinstance(source_package_url, str)
        assert source_package_url in expected_output['package_url']

    def test_get_name(self, package: LinuxPackage, samples: dict):
        expected_output = samples['output']
        name = package.get_name()
        assert isinstance(name, str)
        assert name in expected_output['name']

    def test_get_vendor(self, package: LinuxPackage, samples: dict):
        expected_output = samples['output']
        vendor = package.get_vendor()
        assert isinstance(vendor, str)
        assert vendor in expected_output['vendor']

    def test_get_description(self, package: LinuxPackage, samples: dict):
        expected_output = samples['output']
        description = package.get_description()
        assert isinstance(description, str)
        assert description in expected_output['description']

    def test_get_license_info(self, package: LinuxPackage, samples: dict):
        expected_output = samples['output']
        license_info = package.get_license_info()
        assert isinstance(license_info, LicenseInfo)
        assert set(license_info.content) in expected_output['licenses']
