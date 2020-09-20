package com.dask.sql.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A function (placeholder) in the form, that calcite understands.
 *
 * Basically just a name, a return type and the list of parameters. Please note
 * that this function has no implementation but just serves as a name, which
 * calcite will return to dask_sql again.
 */
public class DaskFunction {
	/// The internal storage of parameters
	private final List<FunctionParameter> parameters;
	/// The internal storage of the return type
	private final SqlTypeName returnType;
	/// The internal storage of the name
	private final String name;

	/// Create a new function out of the given parameters and return type
	public DaskFunction(final String name, final SqlTypeName returnType) {
		this.name = name;
		this.parameters = new ArrayList<FunctionParameter>();
		this.returnType = returnType;
	}

	/// Add a new parameter
	public void addParameter(String name, SqlTypeName parameterSqlType, boolean optional) {
		this.parameters.add(new DaskFunctionParameter(this.parameters.size(), name, parameterSqlType, optional));
	}

	/// Return the name of the function
	public String getFunctionName() {
		return this.name;
	}

	/// The list of parameters of the function
	public List<FunctionParameter> getParameters() {
		return this.parameters;
	}

	/// The return type of the function
	public RelDataType getReturnType(final RelDataTypeFactory relDataTypeFactory) {
		return relDataTypeFactory.createSqlType(this.returnType);
	}

}
