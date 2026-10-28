"""Small JSON contract validator for the supported schema subset.

Checks the JSON Schema subset the contracts use: type (incl. lists and null), const, enum,
required, properties, items, additionalProperties (false or a schema) and minimum.
The discriminator extension selects a sub-schema from mapping by propertyName.
Returns a list of problems; empty means valid. CLI: python3 -I validate.py CONTRACT.json < payload.json (exit 1 if invalid).
"""

import json
import math
import sys

TYPES = {'object': dict, 'array': list, 'string': str, 'boolean': bool, 'null': type(None)}


def is_type(value, name):
    if name == 'number':  # JSON allows no Infinity/NaN; Python decodes 1e309 to inf
        return type(value) is int or (type(value) is float and math.isfinite(value))
    if name == 'integer':
        return type(value) is int
    return type(value) is TYPES[name]


def problems(schema, value, path='$'):
    out = []
    if 'const' in schema and value != schema['const']:
        return [f'{path}: expected {schema["const"]!r}']
    if 'enum' in schema and value not in schema['enum']:
        return [f'{path}: {value!r} not in {schema["enum"]}']
    kinds = schema.get('type')
    if kinds is not None:
        kinds = kinds if isinstance(kinds, list) else [kinds]
        if not any(is_type(value, k) for k in kinds):
            return [f'{path}: expected {"|".join(kinds)}, got {type(value).__name__}']
    if 'minimum' in schema and type(value) in (int, float) and value < schema['minimum']:
        out.append(f'{path}: below {schema["minimum"]}')
    if isinstance(value, dict):
        if 'discriminator' in schema:
            selector = schema['discriminator']
            tag = value.get(selector['propertyName'])
            branch = selector['mapping'].get(tag) if isinstance(tag, str) else None
            if branch is None:
                return [f'{path}: unknown {selector["propertyName"]}']
            out += problems(branch, value, path)
        out += [f'{path}.{k}: missing' for k in schema.get('required', []) if k not in value]
        props = schema.get('properties', {})
        extra = schema.get('additionalProperties')
        for k, v in value.items():
            if k in props:
                out += problems(props[k], v, f'{path}.{k}')
            elif isinstance(extra, dict):
                out += problems(extra, v, f'{path}.{k}')
            elif extra is False:
                out.append(f'{path}.{k}: unexpected')
    if isinstance(value, list) and 'items' in schema:
        for i, v in enumerate(value):
            out += problems(schema['items'], v, f'{path}[{i}]')
    return out


if __name__ == '__main__':
    found = problems(json.load(open(sys.argv[1])), json.load(sys.stdin))
    print('\n'.join(found[:50]) or 'valid')
    sys.exit(1 if found else 0)
