package com.dask.sql.schema;

import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Aggregate function class. See DaskFunction for more description.
 */
public class DaskAggregateFunction extends DaskFunction implements AggregateFunction {
	public DaskAggregateFunction(String name, SqlTypeName returnType) {
		super(name, returnType);
	}
}
