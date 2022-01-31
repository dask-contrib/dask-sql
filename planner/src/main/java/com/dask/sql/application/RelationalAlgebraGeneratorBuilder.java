package com.dask.sql.application;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.dask.sql.schema.DaskSchema;
import org.apache.calcite.plan.RelOptRule;

/**
 * RelationalAlgebraGeneratorBuilder is a Builder-pattern to make creating a
 * RelationalAlgebraGenerator easier from Python.
 */
public class RelationalAlgebraGeneratorBuilder {
	private final String rootSchemaName;
	private final List<DaskSchema> schemas;
	private final boolean case_sensitive; // False if case should be ignored when comparing SQLNode(s)
	private final ArrayList<RelOptRule> disabled_rules; // CBO rules that should be disabled for this context

	public RelationalAlgebraGeneratorBuilder(final String rootSchemaName, final boolean case_sensitive, final ArrayList<RelOptRule> disabled_rules) {
		this.rootSchemaName = rootSchemaName;
		this.schemas = new ArrayList<>();
		this.case_sensitive = case_sensitive;
		this.disabled_rules = disabled_rules;
	}

	public RelationalAlgebraGeneratorBuilder addSchema(final DaskSchema schema) {
		schemas.add(schema);
		return this;
	}

	public RelationalAlgebraGenerator build() throws ClassNotFoundException, SQLException {
		return new RelationalAlgebraGenerator(rootSchemaName, schemas, this.case_sensitive, this.disabled_rules);
	}
}
