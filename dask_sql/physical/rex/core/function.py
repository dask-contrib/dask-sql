from typing import TYPE_CHECKING, Any

from dask_planner.rust import Expression
from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import sql_to_python_value
from dask_sql.physical.rex.base import BaseRexPlugin

if TYPE_CHECKING:
    import dask_sql


class RexScalarFunctionPlugin(BaseRexPlugin):

    class_name = "RexScalarFunction"

    def convert(
        self,
        rex: Expression,
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Any:
        expr_type = rex.get_expr_type()
        print(f"Function Name: {expr_type}")
        print(f"Expression in scalar_function.py: {rex}")
        literal_type = str(rex.getType())
        print(f"literal_type: {literal_type}")

        # Call the Rust function to get the actual value and convert the Rust
        # type name back to a SQL type
        if literal_type == "Boolean":
            literal_type = "BOOLEAN"
            literal_value = rex.getBoolValue()
        elif literal_type == "Float32":
            literal_type = "FLOAT"
            literal_value = rex.getFloat32Value()
        else:
            raise RuntimeError(
                "Failed to determine Datafusion Type in scalar_function.py"
            )

        print(f"Expression in literal.py literal_value: {literal_value}")
        print(f"Expression in literal.py literal_type: {literal_type}")

        # if isinstance(literal_value, org.apache.calcite.util.Sarg):
        #     return SargPythonImplementation(literal_value, literal_type)

        python_value = sql_to_python_value(literal_type, literal_value)
        print(
            f"literal.py python_value: {python_value} or Python type: {type(python_value)}"
        )

        return python_value
