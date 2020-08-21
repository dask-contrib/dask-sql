from dask_sql.java import get_java_class


_plugins = {}

def register_plugin(name, plugin):
    _plugins[name] = plugin


def register_plugin_class(plugin_class):
    _plugins[plugin_class.class_name] = plugin_class


def register_plugin_classes(plugin_classes):
    for plugin_class in plugin_classes:
        register_plugin_class(plugin_class)


def apply_rex_call(rex, df):
    class_name = get_java_class(rex)

    try:
        class_plugin = _plugins[class_name]
    except KeyError:
        raise NotImplementedError(f"No conversion for class {class_name} available (yet).")

    plugin_instance = class_plugin()
    df = plugin_instance(rex, df=df)
    return df


