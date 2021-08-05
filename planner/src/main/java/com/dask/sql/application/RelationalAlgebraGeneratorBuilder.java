package com.dask.sql.application;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.dask.sql.schema.DaskSchema;

public class RelationalAlgebraGeneratorBuilder {
	private final String rootSchemaName;
	private final List<DaskSchema> schemas;

	public RelationalAlgebraGeneratorBuilder(final String rootSchemaName) {
		this.rootSchemaName = rootSchemaName;
		this.schemas = new ArrayList<>();
	}

	public RelationalAlgebraGeneratorBuilder addSchema(final DaskSchema schema) {
		schemas.add(schema);
		return this;
	}

	public RelationalAlgebraGenerator build() throws ClassNotFoundException, SQLException {
		return new RelationalAlgebraGenerator(rootSchemaName, schemas);
	}
}
