package com.dask.sql.application;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

public class DaskSqlDialect {
    public static final RelDataTypeSystem DASKSQL_TYPE_SYSTEM = new RelDataTypeSystemImpl() {
        @Override
        public int getMaxPrecision(SqlTypeName typeName) {
            return PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM.getMaxPrecision(typeName);
        }

        @Override
        public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
            return typeFactory.decimalOf(argumentType);
        }
    };

    public static final SqlDialect.Context DEFAULT_CONTEXT = PostgresqlSqlDialect.DEFAULT_CONTEXT
            .withUnquotedCasing(Casing.UNCHANGED).withDataTypeSystem(DASKSQL_TYPE_SYSTEM);

    // This is basically PostgreSQL, but we would like to keep the casing
    // of unquoted table identifiers, as this is what pandas/dask
    // would also do.
    // See https://github.com/nils-braun/dask-sql/issues/84
    public static final SqlDialect DEFAULT = new PostgresqlSqlDialect(DEFAULT_CONTEXT);
}
