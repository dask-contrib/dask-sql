import numpy as np

from dask_sql.java import SqlTypeName


NP_TO_SQL = {
    # Missing: time and dates, varchar, more complex stuff
    np.dtype("float64"): SqlTypeName.DOUBLE,
    np.dtype("float32"): SqlTypeName.FLOAT,
    np.dtype("int64"): SqlTypeName.BIGINT,
    np.dtype("int32"): SqlTypeName.INTEGER,
    np.dtype("int16"): SqlTypeName.SMALLINT,
    np.dtype("int8"): SqlTypeName.TINYINT,
    np.dtype("uint64"): SqlTypeName.BIGINT,
    np.dtype("uint32"): SqlTypeName.INTEGER,
    np.dtype("uint16"): SqlTypeName.SMALLINT,
    np.dtype("uint8"): SqlTypeName.TINYINT,
    np.dtype("bool8"): SqlTypeName.BOOLEAN,
}

SQL_TO_NP = {
    # Missing: time and dates, varchar, more complex stuff
    "DOUBLE": np.float64,
    "FLOAT": np.float32,
    "BIGINT": np.int64,
    "INTEGER": np.int32,
    "SMALLINT": np.int16,
    "TINYINT": np.int8,
    "BOOLEAN": np.bool8,
}
