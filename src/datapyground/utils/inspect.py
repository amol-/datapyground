"""Provide insights about Python objects."""

import inspect
from typing import Any


def get_qualname(obj: Any) -> str:
    """Get the qualified name of the given object.

    Will return the name of the object and the
    name of the module and class it belongs to.

    For functions or methods, this will return
    something like `module.class.method` or
    `module.function`.

    >>> class TestClass:
    ...   def method(self, arg):
    ...     pass
    >>> get_qualname(TestClass.method)
    'datapyground.utils.inspect.TestClass.method'
    """
    module = inspect.getmodule(obj).__name__
    if inspect.ismethod(obj) or inspect.isfunction(obj):
        if hasattr(obj, "__self__") and obj.__self__:
            class_name = obj.__self__.__class__.__name__
            return f"{module}.{class_name}.{obj.__name__}"
        return f"{module}.{obj.__qualname__}"
    elif inspect.isclass(obj):
        return f"{module}.{obj.__name__}"
    elif inspect.ismodule(obj):
        return obj.__name__
    elif isinstance(obj, object):
        return f"{module}.{obj.__class__.__name__}"
    raise ValueError(f"Unable to detect path for object of type {type(obj)}")
