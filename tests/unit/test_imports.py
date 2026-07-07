import importlib
import pkgutil

import pytest

import flux_producers

MODULES = [m.name for m in pkgutil.iter_modules(flux_producers.__path__, "flux_producers.")]


@pytest.mark.parametrize("module", MODULES)
def test_module_imports(module):
    importlib.import_module(module)
