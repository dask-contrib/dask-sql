/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.dask.sql.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Collections;
import java.util.Map;

import javafx.util.Pair;

public class DaskTable implements ProjectableFilterableTable {
	final static Logger LOGGER = LoggerFactory.getLogger(DaskTable.class);
	private ArrayList<Pair<String, SqlTypeName>> tableColumns;
	private String name;

	public DaskTable(String name) {
		this.name = name;
		this.tableColumns = new ArrayList<Pair<String, SqlTypeName>>();
	}

	public void
	addColumn(String columnName, SqlTypeName columnType) {
		this.tableColumns.add(new Pair<>(columnName, columnType));
	}

	public String
	getTableName() {
		return this.name;
	}

	@Override
	public RelDataType
	getRowType(RelDataTypeFactory rdtf) {
		RelDataTypeFactory.FieldInfoBuilder builder = rdtf.builder();
		for(Pair<String, SqlTypeName> column : tableColumns) {
			String name = column.getKey();
			SqlTypeName type = column.getValue();
			builder.add(name, rdtf.createSqlType(type));
			builder.nullable(true);
		}
		return builder.build();
	}

	@Override
	public Statistic
	getStatistic() {
		return Statistics.UNKNOWN;
	}

	@Override
	public Schema.TableType
	getJdbcTableType() {
		return Schema.TableType.TABLE;
	}

	// private RelDataType
	// convertToSqlType(SqlTypeName dataType, RelDataTypeFactory typeFactory) {
		// return typeFactory.createSqlType(dataType);
		// RelDataType temp = null;
		// switch(dataType) {
		// 	case INT8:
		// 	case UINT8:
		// 		temp = (SqlTypeName.TINYINT);
		// 		break;
		// 	case INT16:
		// 	case UINT16:
		// 		temp = typeFactory.createSqlType(SqlTypeName.SMALLINT);
		// 		break;
		// 	case INT32:
		// 	case UINT32:
		// 		temp = typeFactory.createSqlType(SqlTypeName.INTEGER);
		// 		break;
		// 	case INT64:
		// 	case UINT64:
		// 		temp = typeFactory.createSqlType(SqlTypeName.BIGINT);
		// 		break;
		// 	case FLOAT32:
		// 		temp = typeFactory.createSqlType(SqlTypeName.FLOAT);
		// 		break;
		// 	case FLOAT64:
		// 		temp = typeFactory.createSqlType(SqlTypeName.DOUBLE);
		// 		break;
		// 	case BOOL8:
		// 		temp = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
		// 		break;
		// 	case TIMESTAMP_DAYS:
		// 	case TIMESTAMP_SECONDS:
		// 		temp = typeFactory.createSqlType(SqlTypeName.DATE);
		// 		break;
		// 	case TIMESTAMP_MILLISECONDS:
		// 	case TIMESTAMP_MICROSECONDS:
		// 	case TIMESTAMP_NANOSECONDS:
		// 		temp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
		// 		break;
		// 	case DICTIONARY32:
		// 	case STRING:
		// 		temp = typeFactory.createSqlType(SqlTypeName.VARCHAR);
		// 		break;
		// 	default:
		// 		temp = null;
		// }
		// return temp;
	// }

	@Override
	public boolean
	isRolledUp(String string) {
		return false;
	}

	@Override
	public boolean
	rolledUpColumnValidInsideAgg(String string, SqlCall sc, SqlNode sn, CalciteConnectionConfig ccc) {
		throw new UnsupportedOperationException("rolledUpColumnValidInsideAgg is not supported");
	}


	@Override
	public Enumerable<Object[]>
	scan(DataContext root, List<RexNode> filters, int[] projects) {
		return null;
	}
}
