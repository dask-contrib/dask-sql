package com.dask.sql.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Helper class to store a function parameter in a form that calcite
 * understands.
 */
public class DaskFunctionParameter implements FunctionParameter {
	/// Internal storage of the parameter position
	private final int ordinal;
	/// Internal storage of the parameter name
	private final String name;
	/// Internal storage of the parameter type
	private final SqlTypeName parameterSqlType;
	/// Internal storage if the parameter is optional
	private final boolean optional;

	/// Create a new function parameter from the parameters
	public DaskFunctionParameter(int ordinal, String name, SqlTypeName parameterSqlType, boolean optional) {
		this.ordinal = ordinal;
		this.name = name;
		this.parameterSqlType = parameterSqlType;
		this.optional = optional;
	}

	/// Create a new function parameter from the parameters
	public DaskFunctionParameter(int ordinal, String name, SqlTypeName parameterSqlType) {
		this(ordinal, name, parameterSqlType, false);
	}

	/// The position of this parameter
	@Override
	public int getOrdinal() {
		return this.ordinal;
	}

	/// The name of this parameter
	@Override
	public String getName() {
		return this.name;
	}

	/// Type of the parameter
	@Override
	public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
		return relDataTypeFactory.createSqlType(this.parameterSqlType);
	}

	/// Is the parameter optional=
	@Override
	public boolean isOptional() {
		return this.optional;
	}

}
