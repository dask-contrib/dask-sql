package com.dask.sql.application;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.dask.sql.schema.DaskSchema;

public class RelationalAlgebraGeneratorBuilder {
	private final String rootSchemaName;
	private final List<DaskSchema> schemas;
	private final boolean case_sensitive; // True if case should be ignored when comparing SQLNode(s)

	public RelationalAlgebraGeneratorBuilder(final String rootSchemaName, final boolean case_sensitive) {
		this.rootSchemaName = rootSchemaName;
		this.schemas = new ArrayList<>();
		this.case_sensitive = case_sensitive;
	}

	public RelationalAlgebraGeneratorBuilder addSchema(final DaskSchema schema) {
		schemas.add(schema);
		return this;
	}

	public RelationalAlgebraGenerator build() throws ClassNotFoundException, SQLException {
		return new RelationalAlgebraGenerator(rootSchemaName, schemas, case_sensitive);
	}
}
