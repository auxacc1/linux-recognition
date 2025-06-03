from collections.abc import Mapping
from functools import reduce
from typing import Any


def fetch(
        mapping: Mapping, *keys: Any, default: Any = None, output_type: type | None = None
) -> Any:
    result = reduce(
        lambda m, key: m.get(key, None) if isinstance(m, Mapping) else None, keys, mapping
    )
    if output_type is None or isinstance(result, output_type):
        return result or (default if default is not None else result)
    if default is None:
        type_to_default: dict[type, Any] = {str : '', list: [], Mapping: {}, dict: {}}
        if output_type in type_to_default:
            return type_to_default[output_type]
    return default
