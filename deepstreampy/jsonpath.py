from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

import re
from copy import deepcopy
from deepstreampy.utils import Undefined, num_types

SPLIT_REG_EXP = r"[\.\[\]]"


def get(data, path, deep_copy):
    node = data
    tokens = _tokenize(path)
    for token in tokens:
        try:
            node = node[token]
        except (IndexError, KeyError):
            return None

    if deep_copy:
        return deepcopy(node)
    else:
        return node


def set(data, path, value, deep_copy):
    if deep_copy:
        data = deepcopy(data)
    node = data

    tokens = _tokenize(path)
    if not tokens:
        data = value
        return data

    for i, token in enumerate(tokens[:-1]):
        try:
            node = node[token]
        except (KeyError, IndexError):
            if (i + 1 < len(tokens) and
                    isinstance(tokens[i+1], int)):
                if isinstance(node, list):
                    _pad_list(node, token + 1, None)

                node[token] = []
            else:
                if isinstance(node, list):
                    _pad_list(node, token + 1, None)

                node[token] = {}

            node = node[token]
    last_token = tokens[-1]
    if value is Undefined:
        del node[last_token]
        return data

    if isinstance(node, list):
        _pad_list(node, len(node) + 1, None)

    node[last_token] = value

    return data


def _tokenize(path):
    if not path:
        return ()

    if isinstance(path, num_types):
        return [path]

    parts = re.split(SPLIT_REG_EXP, path)
    tokens = []
    for part in parts:
        if len(part) == 0:
            continue
        try:
            tokens.append(int(part))
        except ValueError:
            if part == '*':
                tokens.append(True)
            else:
                tokens.append(part)

    return tokens


def _pad_list(_list, index, value):
    if len(_list) < index - 1:
        _list.extend([value] * (index - len(_list)))
