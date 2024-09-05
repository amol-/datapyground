import inspect


def get_qualname(obj):
    """Get the qualified name of the given object.

    Will return the name of the object and the
    name of the module and class it belongs to.

    For functions or methods, this will return
    something like `module.class.method` or
    `module.function`.
    """
    module = inspect.getmodule(obj).__name__
    if inspect.ismethod(obj) or inspect.isfunction(obj):
        if hasattr(obj, "__self__") and obj.__self__:
            class_name = obj.__self__.__class__.__name__
            return f"{module}.{class_name}.{obj.__name__}"
        return f"{module}.{obj.__name__}"
    elif inspect.isclass(obj):
        return f"{module}.{obj.__name__}"
    elif inspect.ismodule(obj):
        return obj.__name__
    elif isinstance(obj, object):
        return f"{module}.{obj.__class__.__name__}"
    raise ValueError(f"Unable to detect path for object of type {type(obj)}")