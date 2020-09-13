package com.dask.sql.schema;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Scalar function class. See DaskFunction for more description.
 */
public class DaskScalarFunction extends DaskFunction implements ScalarFunction {
	public DaskScalarFunction(String name, SqlTypeName returnType) {
		super(name, returnType);
	}
}
