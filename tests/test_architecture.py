import ast
import inspect

import pytest

from smartdialer import pacing
from smartdialer.safety import Permit


def test_pacing_import_boundary():
    forbidden = {"allocator", "router", "providers", "repository"}
    for node in ast.walk(ast.parse(inspect.getsource(pacing))):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name for alias in node.names]
            if isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
            assert all(forbidden.isdisjoint(name.split(".")) for name in names)


def test_only_controller_can_construct_permit():
    with pytest.raises(TypeError):
        Permit("campaign", 1, 100, "token")
    with pytest.raises(TypeError):
        Permit("campaign", 1, 100, "token", object())
