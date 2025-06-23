import re
from calendar import month_name, month_abbr, monthrange
from functools import reduce
from itertools import permutations, repeat

from linux_recognition.typestore.datatypes import (
    Date,
    DateLikeParse,
    DatePatterns,
    DatePatternsComplete,
    DatePatternsNoYear,
    ParsedDateLike
)


def parse_date_from_digits(digits_match: re.Match) -> Date | None:
    groups = digits_match.groupdict()
    year = int(groups['y'])
    month = int(groups['m'])
    if not 1 <= month <= 12:
        return None
    days_in_month = monthrange(year, month)[1]
    day_group = groups['d']
    if day_group is None:
        return Date(year, month, days_in_month)
    day = int(day_group)
    if not 1 <= day <= days_in_month:
        return None
    return Date(year, month, day)


def parse_date(
        text: str = '',
        patterns: DatePatterns | None = None,
        reference_collection: list | None = None,
        date_like: DateLikeParse | None = None,
        no_year: bool = False
) -> Date | None:
    if reference_collection is None:
        reference_collection = []
    if no_year:
        return _parse_no_year_date(text, patterns, date_like=date_like)
    if date_like is None:
        if not text or patterns is None:
            return None
        date_like = extract_date_like(text, patterns)
        if date_like is None:
            return None
    parsed = date_like.parsed
    if parsed is None:
        return None
    if date_like.ordered:
        day = parsed.non_year_1 if parsed.non_year_1 is not None else 1
        return Date(parsed.year, parsed.non_year_0, day)
    year = parsed.year
    non_year_parts = parsed[1:]
    if non_year_parts[1] is None:
        return None
    day_index = next((ind for ind, part in enumerate(non_year_parts) if part > 12), None)
    if day_index is not None:
        day = non_year_parts[day_index]
        month = non_year_parts[day_index - 1]
        if not 1 <= month <= 12 or day > monthrange(year, month)[1]:
            return None
        return Date(year, month, day)
    for string in reference_collection:
        if not string:
            continue
        _date_like = extract_date_like(string, patterns)
        _parsed = _date_like.parsed
        _non_year_parts = _parsed[1:]
        day_index = next((non_year_parts.index(part) for part in _non_year_parts if part > 12), None)
        if day_index:
            break
    if day_index is not None:
        day = non_year_parts[day_index]
        month = non_year_parts[day_index + 1 % 2]
        if not 1 <= month <= 12 or day  > monthrange(year, month)[1]:
            return None
        return Date(year, month, day)
    matched = date_like.match.group()
    if '/' in matched:
        day, month = non_year_parts
    elif '-' in matched:
        day, month = reversed(non_year_parts)
    else:
        day, month = None, None
    if day is None or month not in range(1, 13) or day > monthrange(year, month)[1]:
        return None
    return Date(year, month, day)


def extract_date_like(
        text: str, patterns: DatePatterns, no_year: bool = False
) -> DateLikeParse | None:
    if no_year:
        return _extract_no_year_date_like(text, patterns.no_year)
    date_patterns = patterns.complete
    date_like_match = date_patterns.digital.search(text)
    if date_like_match is not None:
        group_dict = date_like_match.groupdict()
        matched_groups = [key for key in group_dict if group_dict[key]]
        non_year = [int(group_dict[name]) for name in matched_groups if 'non_year' in name]
        year = next(int(group_dict[name]) for name in matched_groups if 'non_year' not in name)
        parsed = ParsedDateLike(year, *non_year)
        return DateLikeParse(parsed=parsed, ordered=False, match=date_like_match)
    date_like_match = date_patterns.word_month.search(text)
    if date_like_match is not None:
        group_dict = date_like_match.groupdict()
        matched_groups = [key for key in group_dict if group_dict[key]]
        month = list(month_abbr).index(
            group_dict[
                next(n for n in matched_groups if 'month' in n)
            ][:3].title()
        )
        year, day = [
            next(int(group_dict[name]) for name in matched_groups if p in name) for p in ['year','day']
        ]
        parsed = ParsedDateLike(year, month, day)
        return DateLikeParse(parsed=parsed, ordered=True, match=date_like_match)
    date_like_match = date_patterns.no_separator.search(text)
    if date_like_match is None:
        return None
    group_dict = date_like_match.groupdict()
    matched_groups = [key for key in group_dict if group_dict[key]]
    if matched_groups:
        parsed = ParsedDateLike(
            *[next(int(group_dict[name])
                   for name in matched_groups if name.startswith(p)) for p in ['year', 'month', 'day']]
        )
        return DateLikeParse(parsed=parsed, ordered=True, match=date_like_match)
    matched = date_like_match.group()
    non_year_count = 2 if len(matched) == 8 else 1
    if 1900 < int(matched[-4:]) < 2100:
        year = int(matched[-4:])
        non_year = (int(matched[0 + 2 * j:2 + 2 * j]) for j in range(non_year_count))
        parsed = ParsedDateLike(year, *non_year)
    elif 1900 < int(matched[:4]) < 2100:
        year = int(matched[:4])
        non_year = (int(matched[4:][0 + 2 * j:2 + 2 * j]) for j in range(non_year_count))
        parsed = ParsedDateLike(year, *non_year)
    else:
        return None
    ordered = False if non_year_count == 2 else True
    return DateLikeParse(parsed=parsed, ordered=ordered, match=date_like_match)


def generate_complete_date_patterns() -> DatePatternsComplete:
    delimiter_patterns = [fr'[\s\n]*{sep}[\s\n]*' for sep in [r',', r'\-', r'\/', r'\.', r'\s']]
    digital_parts_patterns = [*repeat(r'(?P<non_year_>\b\d{1,2}\b)', 2), r'(?P<year_>\b\d{4}\b)']
    digital_patterns = [
        fr'{d.join(p)}' for d in delimiter_patterns for p in set(permutations(digital_parts_patterns))
    ]
    digital_patterns = [
        reduce(
            lambda pat, substr: _get_replacement(pat, substr, index),
            [f'P<{p}_>' for p in ('year', 'non_year')],
            pattern
        ) for (index, pattern) in enumerate(digital_patterns)
    ]
    digital_pattern = re.compile(r'|'.join(digital_patterns))
    months_pattern = r'|'.join(month_name[1:])
    months_abbr_pattern = r'|'.join(month_abbr[1:])
    word_month_parts_patterns = [
        r'(?P<day_>\b\d{1,2}\b)',
        fr'(?P<month_>\b{months_abbr_pattern}|{months_pattern}\b)',
        r'(?P<year_>\b\d{4}\b)'
    ]
    word_month_patterns = [
        fr'{d.join(p)}' for d in delimiter_patterns for p in set(permutations(word_month_parts_patterns))
    ]
    word_month_patterns = [
        reduce(
            lambda pat, substr: _get_replacement(pat, substr, index, word_month=True),
            [f'P<{p}_>' for p in ('year', 'month', 'day')],
            pattern
        )
        for index, pattern in enumerate(word_month_patterns)
    ]
    word_month_pattern = re.compile(r'|'.join(word_month_patterns), flags=re.IGNORECASE)
    no_separator_pattern = re.compile(
        fr'\b{'|'.join(
            [
                fr'(?P<day_0>\d{{1,2}})(?P<month_0>{months_abbr_pattern}|{months_pattern})(?P<year_0>\d{{4}})',
                fr'(?P<year_1>\d{{4}})(?P<month_1>{months_abbr_pattern}|{months_pattern})(?P<day_1>\d{{1,2}})',
                r'\d{6}(?:d{2})?'
            ]
        )}\b',
        flags=re.IGNORECASE
    )
    return DatePatternsComplete(digital_pattern, word_month_pattern, no_separator_pattern)


def generate_no_year_patterns() -> DatePatternsNoYear:
    delimiter_patterns = [fr'[\s\n]*{sep}[\s\n]*' for sep in [r',', r'\-', r'\/', r'\.', r'\s']]
    digital_parts_patterns = [r'(?P<non_year_a_>\b\d{1,2}\b)', r'(?P<non_year_b_>\b\d{1,2}\b)']
    digital_patterns =  [
        fr'{d.join(digital_parts_patterns)}'.replace('_>',f'_{ind}>')
        for (ind, d) in enumerate(delimiter_patterns)
    ]
    digital_pattern = re.compile(r'|'.join(digital_patterns))
    months_pattern = r'|'.join(month_name[1:])
    months_abbr_pattern = r'|'.join(month_abbr[1:])
    word_month_parts_patterns = [
        r'(?P<day_>\b\d{1,2}\b)', fr'(?P<month_>\b{months_abbr_pattern}|{months_pattern}\b)'
    ]
    patterns_permutations = [word_month_parts_patterns, list(reversed(word_month_parts_patterns))]
    word_month_patterns = [
        fr'{d.join(patterns_permutations[j])}'.replace(
            '_>',f'_{ind + j * len(delimiter_patterns) }>'
        )
        for j in range(2)for (ind, d) in enumerate(delimiter_patterns)
    ]
    word_month_pattern = re.compile(r'|'.join(word_month_patterns), flags=re.IGNORECASE)
    return DatePatternsNoYear(digital_pattern, word_month_pattern)


def _parse_no_year_date(
        text: str,
        date_patterns: DatePatterns,
        date_like: DateLikeParse | None = None
) -> Date | None:
    if date_like is None:
        if date_patterns is None:
            return None
        date_like = _extract_no_year_date_like(text, date_patterns.no_year)
        if date_like is None:
            return None
    parsed = date_like.parsed
    if parsed is None:
        return None
    if date_like.ordered:
        return Date(*parsed)
    non_year_parts = parsed[1:]
    day = next((part for part in non_year_parts if part > 12), None)
    if day is not None:
        month = next(part for part in non_year_parts if part != day)
        if not 1 <= month <= 12 or day > monthrange(2000, month)[1]:
            return None
        return Date(parsed[0], month, day)
    return None


def _extract_no_year_date_like(text: str, patterns: DatePatternsNoYear) -> DateLikeParse | None:
    date_like_match = patterns.digital.search(text)
    if date_like_match is not None:
        group_dict = date_like_match.groupdict()
        matched_groups = [key for key in group_dict if group_dict[key]]
        non_year = [int(group_dict[name]) for name in matched_groups]
        parsed =  ParsedDateLike(None, *non_year)
        return DateLikeParse(parsed=parsed, ordered=False, match=date_like_match, mo_year=True)
    date_like_match = patterns.word_month.search(text)
    if date_like_match is not None:
        group_dict = date_like_match.groupdict()
        matched_groups = sorted([key for key in group_dict if group_dict[key]])
        parsed = ParsedDateLike(
            year=None,
            non_year_0=list(month_abbr).index(group_dict[matched_groups[1]][:3].title()),
            non_year_1=int(group_dict[matched_groups[0]])
        )
        return DateLikeParse(parsed=parsed, ordered=True, match=date_like_match, mo_year=True)
    return None


def _get_replacement(pattern: str, substring: str, index: int, word_month: bool = False) -> str:
    if word_month:
        replacements = {
            f'P<{part}_>': f'P<{part}_{index}>' for part in ('year', 'month', 'day')
        }
        return pattern.replace(substring, replacements[substring])
    replacements = {
        'P<year_>': f'P<year_{index}>',
        'P<non_year_>': [f'P<non_year_{p}_{index}>' for p in {'a', 'b'}]
    }
    match substring:
        case 'P<non_year_>':
            return pattern.replace(substring, replacements[substring][0], 1).replace(
                substring, replacements[substring][1], 1)
        case 'P<year_>':
            return pattern.replace(substring, replacements[substring])
    return pattern
