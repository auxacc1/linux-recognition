# WARNING: This module is a temporary workaround. It is a fork of the python-rpm-spec library.
# Repository: https://github.com/bkircher/python-rpm-spec
# It is not intended to be a permanent solution.

import os
import re
from abc import ABCMeta, abstractmethod
from typing import Any, AnyStr, Dict, List, Optional, Union, Tuple, Type, cast
from warnings import warn

__all__ = ["Spec", "replace_macros", "Package", "warnings_enabled"]


# Set this to True if you want the library to issue warnings during parsing.
warnings_enabled: bool = False


class _Tag(metaclass=ABCMeta):
    def __init__(self, name: str, pattern_obj: re.Pattern, attr_type: Type[Any]) -> None:
        self.name = name
        self.pattern_obj = pattern_obj
        self.attr_type = attr_type

    def test(self, line: str) -> Optional[re.Match]:
        return re.search(self.pattern_obj, line)

    def update(self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str) -> Any:
        """Update given spec object and parse context and return them again.

        :param spec_obj: An instance of Spec class
        :param context: The parse context
        :param match_obj: The re.match object
        :param line: The original line
        :return: Given updated Spec instance and parse context dictionary.
        """

        assert spec_obj
        assert context
        assert match_obj
        assert line

        return self.update_impl(spec_obj, context, match_obj, line)

    @abstractmethod
    def update_impl(
            self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str
    ) -> Tuple["Spec", dict]:
        pass

    @staticmethod
    def current_target(spec_obj: "Spec", context: Dict[str, Any]) -> Union["Spec", "Package"]:
        target_obj = spec_obj
        if context["current_subpackage"] is not None:
            target_obj = context["current_subpackage"]
        return target_obj


class _NameValue(_Tag):
    """Parse a simple name → value tag."""

    def __init__(
            self, name: str, pattern_obj: re.Pattern, attr_type: Optional[Type[Any]] = None
    ) -> None:
        super().__init__(name, pattern_obj, cast(Type[Any], attr_type if attr_type else str))

    def update_impl(
            self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str
    ) -> Tuple["Spec", dict]:
        target_obj = _Tag.current_target(spec_obj, context)
        value = match_obj.group(1)

        # Sub-packages
        if self.name == "name":
            spec_obj.packages = []
            spec_obj.packages.append(Package(value))
        if self.name == 'license' and value:
            spdx_license_prefix_pattern = r'LicenseRef\-[^-]+\-'
            value = re.sub(spdx_license_prefix_pattern, '', value)
            # pattern = r'(?:and|^)\s*(\()?(?P<license>(?:(?!\s*and).)*)(?(1)\))'
            # value = [item['license'] for item in re.finditer(pattern, license_string, re.IGNORECASE)]
        if self.name in ["_description", "description", "changelog"]:
            if self.name == "description":
                suffix = value.strip()
                if suffix and suffix != '%_description':
                    # Attempt to find the corresponding subpackage
                    possible_names = [f"{spec_obj.name}-{suffix}", suffix]
                    for pkg in spec_obj.packages:
                        if pkg.name in possible_names:
                            context["current_subpackage"] = pkg
                            break
                    else:
                        # No subpackage found; reset to main package
                        context["current_subpackage"] = None
                        if warnings_enabled:
                            warn(f"No subpackage found for description suffix '{suffix}'")
                else:
                    # Main description, reset to main package
                    context["current_subpackage"] = None
                    if suffix == '%_description':
                        setattr(target_obj, self.name, self.attr_type(value))

            context["multiline"] = self.name
        else:
            setattr(target_obj, self.name, self.attr_type(value))
        return spec_obj, context


class _SetterMacroDef(_Tag):
    """Parse global macro definitions."""

    def __init__(self, name: str, pattern_obj: re.Pattern) -> None:
        super().__init__(name, pattern_obj, str)

    @abstractmethod
    def get_namespace(self, spec_obj: "Spec", context: Dict[str, Any]) -> "Spec":
        raise NotImplementedError()

    def update_impl(
            self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str
    ) -> Tuple["Spec", dict]:
        name, value = match_obj.groups()
        setattr(self.get_namespace(spec_obj, context), name, str(value))
        return spec_obj, context


class _GlobalMacroDef(_SetterMacroDef):
    """Parse global macro definitions."""

    def get_namespace(self, spec_obj: "Spec", context: Dict[str, Any]) -> "Spec":
        return spec_obj


class _LocalMacroDef(_SetterMacroDef):
    """Parse define macro definitions."""

    def get_namespace(self, spec_obj: "Spec", context: Dict[str, Any]) -> "Spec":
        return context["current_subpackage"]


class _MacroDef(_Tag):
    """Parse global macro definitions."""

    def __init__(self, name: str, pattern_obj: re.Pattern) -> None:
        super().__init__(name, pattern_obj, str)

    def update_impl(
            self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str
    ) -> Tuple["Spec", dict]:
        name, value = match_obj.groups()
        if name in ["description", "_description"]:
            context["multiline"] = name
        else:
            value_as_string = str(value)
            spec_obj.macros[name] = value_as_string
            setattr(spec_obj, name, value_as_string)
        return spec_obj, context


class _List(_Tag):
    """Parse a tag that expands to a list."""

    def __init__(self, name: str, pattern_obj: re.Pattern) -> None:
        super().__init__(name, pattern_obj, list)

    def update_impl(
            self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str
    ) -> Tuple["Spec", dict]:
        target_obj = _Tag.current_target(spec_obj, context)

        if not hasattr(target_obj, self.name):
            setattr(target_obj, self.name, [])

        value = match_obj.group(1)
        if self.name == "packages":
            if value == "-n":
                subpackage_name = re.split(r"\s+", line)[-1].rstrip()
            else:
                subpackage_name = f"{spec_obj.name}-{value}"
            package = Package(subpackage_name)
            context["current_subpackage"] = package
            package.is_subpackage = True
            spec_obj.packages.append(package)
        elif self.name in [
            "build_requires",
            "requires",
            "conflicts",
            "obsoletes",
            "provides",
        ]:
            # Remove comments on same line
            value = value.split("#", 2)[0].rstrip()
            # Macros are valid in requirements
            value = replace_macros(value, spec=spec_obj)

            # It's also legal to do:
            #   Requires: a b c
            #   Requires: b >= 3.1
            #   Requires: a, b >= 3.1, c

            # 1. Tokenize
            tokens = [val for val in re.split("[\t\n, ]", value) if val]
            values: List[str] = []

            # 2. Join
            add = False
            for val in tokens:
                if add:
                    add = False
                    val = values.pop() + " " + val
                elif val in [">=", "!=", ">", "<", "<=", "==", "="]:
                    add = True  # Add next value to this one
                    val = values.pop() + " " + val
                values.append(val)

            for val in values:
                requirement = Requirement(val)
                getattr(target_obj, self.name).append(requirement)
        else:
            getattr(target_obj, self.name).append(value)

        return spec_obj, context


class _ListAndDict(_Tag):
    """Parse a tag that expands to a list and to a dict."""

    def __init__(self, name: str, pattern_obj: re.Pattern) -> None:
        super().__init__(name, pattern_obj, list)

    def update_impl(self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str) -> Tuple["Spec", dict]:
        source_name, value = match_obj.groups()
        dictionary = getattr(spec_obj, f"{self.name}_dict")
        dictionary[source_name] = value
        target_obj = _Tag.current_target(spec_obj, context)
        # If we are in a subpackage, add sources and patches to the subpackage dicts as well
        if hasattr(target_obj, "is_subpackage") and target_obj.is_subpackage:
            dictionary = getattr(target_obj, f"{self.name}_dict")
            dictionary[source_name] = value
        return spec_obj, context


class _SplitValue(_NameValue):
    """Parse a (name->value) tag, and at the same time split the tag to a list."""

    def __init__(self, name: str, pattern_obj: re.Pattern, sep: Optional[None] = None) -> None:
        super().__init__(name, pattern_obj)
        self.name_list = f"{name}_list"
        self.sep = sep

    def update_impl(self, spec_obj: "Spec", context: Dict[str, Any], match_obj: re.Match, line: str) -> Tuple["Spec", dict]:
        super().update_impl(spec_obj, context, match_obj, line)

        target_obj = _Tag.current_target(spec_obj, context)
        value: str = getattr(target_obj, self.name)
        values = value.split(self.sep)
        setattr(target_obj, self.name_list, values)

        return spec_obj, context


def re_tag_compile(tag: AnyStr) -> re.Pattern:
    return re.compile(tag, re.IGNORECASE)


class _DummyMacroDef(_Tag):
    """Parse global macro definitions."""

    def __init__(self, name: str, pattern_obj: re.Pattern) -> None:
        super().__init__(name, pattern_obj, str)

    def update_impl(self, spec_obj: "Spec", context: Dict[str, Any], _: re.Match, line: str) -> Tuple["Spec", dict]:
        context["line_processor"] = None
        if warnings_enabled:
            warn("Unknown macro: " + line)
        return spec_obj, context


_tags = [
    _NameValue("name", re_tag_compile(r"^Name\s*:\s*(\S+)")),
    _NameValue("version", re_tag_compile(r"^Version\s*:\s*(\S+)")),
    _NameValue("epoch", re_tag_compile(r"^Epoch\s*:\s*(\S+)")),
    _NameValue("release", re_tag_compile(r"^Release\s*:\s*(\S+)")),
    _NameValue("summary", re_tag_compile(r"^Summary\s*:\s*(.+)")),
    _NameValue("description", re_tag_compile(r"^%description\s*((?<=\s)[^\s\\]+|)")),
    _NameValue("changelog", re_tag_compile(r"^%changelog\s*(\S*)")),
    _NameValue("license", re_tag_compile(r"^License\s*:\s*(.+)")),
    _NameValue("group", re_tag_compile(r"^Group\s*:\s*(.+)")),
    _NameValue("url", re_tag_compile(r"^URL\s*:\s*(\S+)")),
    _NameValue("buildroot", re_tag_compile(r"^BuildRoot\s*:\s*(\S+)")),
    _SplitValue("buildarch", re_tag_compile(r"^BuildArch\s*:\s*(\S+)")),
    _SplitValue("excludearch", re_tag_compile(r"^ExcludeArch\s*:\s*(.+)")),
    _SplitValue("exclusivearch", re_tag_compile(r"^ExclusiveArch\s*:\s*(.+)")),
    _ListAndDict("sources", re_tag_compile(r"^(Source\d*\s*):\s*(.+)")),
    _ListAndDict("patches", re_tag_compile(r"^(Patch\d*\s*):\s*(\S+)")),
    _List("build_requires", re_tag_compile(r"^BuildRequires\s*:\s*(.+)")),
    _List("requires", re_tag_compile(r"^Requires\s*:\s*(.+)")),
    _List("conflicts", re_tag_compile(r"^Conflicts\s*:\s*(.+)")),
    _List("obsoletes", re_tag_compile(r"^Obsoletes\s*:\s*(.+)")),
    _List("provides", re_tag_compile(r"^Provides\s*:\s*(.+)")),
    _List("packages", re_tag_compile(r"^%package\s+(\S+)")),
    _MacroDef("define", re_tag_compile(r"^%define\s+(\S+)\s+(\S+)")),
    _MacroDef("global", re_tag_compile(r"^%global\s+([^\s\\]+)\s*((?<=\s)[^\s\\]+|(?=\\))")),
    _DummyMacroDef("dummy", re_tag_compile(r"^%[a-z_]+\b.*$")),
]

_tag_names = [tag.name for tag in _tags]

_macro_pattern = re.compile(r"%{(\S+?)}|%(\w+?)\b")


def _parse(spec_obj: "Spec", context: Dict[str, Any], line: str) -> Any:
    for tag in _tags:
        match = tag.test(line)
        if match:
            if "multiline" in context:
                context.pop("multiline", None)
            return tag.update(spec_obj, context, match, line)
    if "multiline" in context:
        if isinstance(line, str) and line.startswith('#'):
            context.pop("multiline", None)
            return spec_obj, context
        target_obj = _Tag.current_target(spec_obj, context)
        previous_txt = getattr(target_obj, context["multiline"], "")
        if previous_txt is None:
            previous_txt = ""
        formatted_line = line.strip('\\')
        formatted_line = formatted_line + ' ' if formatted_line else formatted_line + os.linesep
        setattr(target_obj, context["multiline"], str(previous_txt) + formatted_line)

    return spec_obj, context


class Requirement:
    """Represents a single requirement or build requirement in an RPM spec file.

    Each spec file contains one or more requirements or build requirements.
    For example, consider following spec file::

        Name:           foo
        Version:        0.1

        %description
        %{name} is the library that everyone needs.

        %package devel
        Summary: Header files, libraries and development documentation for %{name}
        Group: Development/Libraries
        Requires: %{name}%{?_isa} = %{version}-%{release}
        BuildRequires: gstreamer%{?_isa} >= 0.1.0

        %description devel
        This package contains the header files, static libraries, and development
        documentation for %{name}. If you like to develop programs using %{name}, you
        will need to install %{name}-devel.

    This spec file's requirements have a name and either a required or minimum
    version.
    """

    expr = re.compile(r"(.*?)\s+([<>]=?|=)\s+(\S+)")

    def __init__(self, name: str) -> None:
        assert isinstance(name, str)
        self.line = name
        self.name: str
        self.operator: Optional[str]
        self.version: Optional[str]
        match = Requirement.expr.match(name)
        if match:
            self.name = match.group(1)
            self.operator = match.group(2)
            self.version = match.group(3)
        else:
            self.name = name
            self.operator = None
            self.version = None

    def __eq__(self, o: object) -> bool:
        if isinstance(o, str):
            return self.line == o
        if isinstance(o, Requirement):
            return self.name == o.name and self.operator == o.operator and self.version == o.version
        return False

    def __repr__(self) -> str:
        return f"Requirement('{self.line}')"


class Package:
    """Represents a single package in a RPM spec file.

    Each spec file describes at least one package and can contain one or more subpackages (described
    by the %package directive). For example, consider following spec file::

        Name:           foo
        Version:        0.1

        %description
        %{name} is the library that everyone needs.

        %package devel
        Summary: Header files, libraries and development documentation for %{name}
        Group: Development/Libraries
        Requires: %{name}%{?_isa} = %{version}-%{release}

        %description devel
        This package contains the header files, static libraries, and development
        documentation for %{name}. If you like to develop programs using %{name}, you
        will need to install %{name}-devel.

        %package -n bar
        Summary: A command line client for foo.
        License: GPLv2+

        %description -n bar
        This package contains a command line client for foo.

    This spec file will create three packages:

    * A package named foo, the base package.
    * A package named foo-devel, a subpackage.
    * A package named bar, also a subpackage, but without the foo- prefix.

    As you can see above, the name of a subpackage normally includes the main package name. When the
    -n option is added to the %package directive, the prefix of the base package name is omitted and
    a completely new name is used.

    """

    def __init__(self, name: str) -> None:
        assert isinstance(name, str)

        for tag in _tags:
            if tag.attr_type is list and tag.name in [
                "build_requires",
                "requires",
                "conflicts",
                "obsoletes",
                "provides",
                "sources",
                "patches",
            ]:
                setattr(self, tag.name, tag.attr_type())
            elif tag.name in [
                "description",
            ]:
                setattr(self, tag.name, None)

        self.sources_dict: Dict[str, str] = {}
        self.patches_dict: Dict[str, str] = {}
        self.name = name
        self.is_subpackage = False

    def __repr__(self) -> str:
        return f"Package('{self.name}')"


class Spec:
    """Represents a single spec file."""

    def __init__(self) -> None:
        for tag in _tags:
            if tag.attr_type is list:
                setattr(self, tag.name, tag.attr_type())
            else:
                setattr(self, tag.name, None)

        self.sources_dict: Dict[str, str] = {}
        self.patches_dict: Dict[str, str] = {}
        self.macros: Dict[str, str] = {}

        self.name: str = ''
        self.packages: List[Package] = []

    @property
    def packages_dict(self) -> Dict[str, Package]:
        """All packages in this RPM spec as a dictionary.

        You can access the individual packages by their package name, e.g.,

        git_spec.packages_dict['git-doc']

        """
        assert self.packages
        return dict(zip([package.name for package in self.packages], self.packages))

    @classmethod
    def from_file(cls, filename: str) -> "Spec":
        """Creates a new Spec object from a given file.

        :param filename: The path to the spec file.
        :return: A new Spec object.
        """

        spec = cls()
        with open(filename, "r", encoding="utf-8") as f:
            parse_context = {"current_subpackage": None}
            for line in f:
                spec, parse_context = _parse(spec, parse_context, line.rstrip())
        return spec

    @classmethod
    def from_string(cls, string: str) -> "Spec":
        """Creates a new Spec object from a given string.

        :param string: The contents of a spec file.
        :return: A new Spec object.
        """

        spec = cls()
        parse_context = {"current_subpackage": None}
        for line in string.splitlines():
            spec, parse_context = _parse(spec, parse_context, line)
        return spec


def replace_macros(string: str, spec: Spec, max_attempts: int = 1000) -> str:
    """Replace all macros in given string with corresponding values.

    For example, a string '%{name}-%{version}.tar.gz' will be transformed to 'foo-2.0.tar.gz'.

    :param string A string containing macros that you want to be replaced.
    :param spec A Spec object. Definitions in that spec file will be used to replace macros.
    :param max_attempts If reached, raises a RuntimeError.

    :return A string where all macros in given input are substituted as good as possible.

    """
    assert isinstance(spec, Spec)

    def get_first_non_none_value(values: Tuple[Any, ...]) -> Any:
        return next((v for v in values if v is not None), None)

    def is_conditional_macro(macro: str) -> bool:
        return macro.startswith(("?", "!"))

    def is_optional_macro(macro: str) -> bool:
        return macro.startswith("?")

    def is_negation_macro(macro: str) -> bool:
        return macro.startswith("!")

    def get_replacement_string(match: re.Match) -> str:
        # pylint: disable=too-many-return-statements
        groups = match.groups()
        macro_name: str = get_first_non_none_value(groups)
        assert macro_name, "Expected a non None value"
        if is_conditional_macro(macro_name) and spec:
            parts = macro_name[1:].split(sep=":", maxsplit=1)
            assert parts, "Expected a ':' in macro name'"
            macro = parts[0]
            if is_optional_macro(macro_name):
                if hasattr(spec, macro) or macro in spec.macros:
                    if len(parts) == 2:
                        return parts[1]

                    if macro in spec.macros:
                        return spec.macros[macro]

                    if hasattr(spec, macro):
                        return getattr(spec, macro)

                    assert False, "Unreachable"

                return ""

            if is_negation_macro(macro_name):
                if len(parts) == 2:
                    return parts[1]

                return spec.macros.get(macro, getattr(spec, macro))

        if spec:
            value = spec.macros.get(macro_name, getattr(spec, macro_name, None))
            if value:
                return str(value)

        return match.string[match.start() : match.end()]

    # Recursively expand macros.
    # Note: If macros are not defined in the spec file, this won't try to
    # expand them.
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        ret = re.sub(_macro_pattern, get_replacement_string, string)
        if ret != string:
            string = ret
            continue
        return ret

    raise RuntimeError("max_attempts reached. Aborting")
