import operator

import jpype
import numpy as np


jpype.addClassPath("./planner/application/target/DaskSQL.jar")
jpype.startJVM("-ea", convertStrings=False)

ColumnTypeClass = jpype.JClass("org.apache.calcite.sql.type.SqlTypeName")
TableClass = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskSchemaClass = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGeneratorClass = jpype.JClass("com.dask.sql.application.RelationalAlgebraGenerator")

tables = {}


mapping = {
    # Missing: time and dates, varchar, more complex stuff
    np.dtype("float64"): ColumnTypeClass.DOUBLE,
    np.dtype("float32"): ColumnTypeClass.FLOAT,
    np.dtype("int64"): ColumnTypeClass.BIGINT,
    np.dtype("int32"): ColumnTypeClass.INTEGER,
    np.dtype("int16"): ColumnTypeClass.SMALLINT,
    np.dtype("int8"): ColumnTypeClass.TINYINT,
    np.dtype("uint64"): ColumnTypeClass.BIGINT,
    np.dtype("uint32"): ColumnTypeClass.INTEGER,
    np.dtype("uint16"): ColumnTypeClass.SMALLINT,
    np.dtype("uint8"): ColumnTypeClass.TINYINT,
    np.dtype("bool8"): ColumnTypeClass.BOOLEAN,
}

def add_dask_table(df, name):
    tables[name] = df


def get_ral(sql):
    schema = DaskSchemaClass("main")

    for name, df in tables.items():
        table = TableClass(name)
        for order, column in enumerate(df.columns):
            data_type = df[column].dtype
            sql_data_type = mapping[data_type]

            table.addColumn(column, sql_data_type)

        schema.addTable(table)

    generator = RelationalAlgebraGeneratorClass(schema)
    print(generator.getRelationalAlgebraString(sql))
    ral = generator.getRelationalAlgebra(sql)

    return ral

def apply_ral(ral):
    try:
        input_ral = ral.getInput()
        df = apply_ral(input_ral)
    except TypeError:
        df = None

    class_name = ral.getClass().getName()

    if class_name == "org.apache.calcite.rel.logical.LogicalTableScan":
        assert df is None

        table = ral.getTable()
        hints = ral.getHints()

        # TODO: hints?

        table_names = [str(n) for n in table.getQualifiedName()]
        assert table_names[0] == "main"
        assert len(table_names) == 2
        table_name = table_names[1]

        row_type = table.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]

        return tables[table_name][field_specifications]

    elif class_name == "org.apache.calcite.rel.logical.LogicalFilter":
        assert df is not None

        variable_set = ral.getVariablesSet()
        condition = ral.getCondition()

        condition = apply_rex_call(condition, df)

        return df[condition]

    else:
        raise NotImplementedError(class_name)


def apply_rex_call(rex_call, df):
    class_name = rex_call.getClass().getName()

    if class_name == "org.apache.calcite.rex.RexLiteral":
        # TODO
        return int(str(rex_call.getValue()))
    elif class_name == "org.apache.calcite.rex.RexInputRef":
        index = rex_call.getIndex()
        return df.iloc[:, index]

    assert class_name == "org.apache.calcite.rex.RexCall"

    operator_name = str(rex_call.getOperator().getName())
    operands = [apply_rex_call(o, df) for o in rex_call.getOperands()]

    if operator_name == "AND":
        assert len(operands) == 2
        return operands[0] & operands[1]
    if operator_name == ">":
        assert len(operands) == 2
        return operands[0] > operands[1]
    if operator_name == "<":
        assert len(operands) == 2
        return operands[0] < operands[1]
    else:
        raise NotImplementedError(operator_name)


