from dask_sql.java import get_java_class


_plugins = {}


def register_plugin_class(plugin_class):
    _plugins[plugin_class.class_name] = plugin_class


def convert_ral_to_df(ral, tables):
    class_name = get_java_class(ral)

    try:
        class_plugin = _plugins[class_name]
    except KeyError:  # pragma: no cover
        raise NotImplementedError(
            f"No conversion for class {class_name} available (yet)."
        )

    plugin_instace = class_plugin()
    df = plugin_instace(ral, tables=tables)
    return df


def fix_column_to_row_type(df, row_type):
    field_names = [str(x) for x in row_type.getFieldNames()]

    df = df.rename(columns=dict(zip(df.columns, field_names)))

    # TODO: types!
    # TODO: we could even us a similar function to test the input of the table
    return df


def check_columns_from_row_type(df, row_type):
    field_names = [str(x) for x in row_type.getFieldNames()]

    assert list(df.columns) == field_names

    # TODO: types!
    # TODO: we could even us a similar function to test the input of the table
    return df
