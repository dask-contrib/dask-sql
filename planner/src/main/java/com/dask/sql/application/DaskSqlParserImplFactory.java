package com.dask.sql.application;

import java.io.Reader;

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import com.dask.sql.parser.DaskSqlParserImpl;

public class DaskSqlParserImplFactory implements SqlParserImplFactory {

    @Override
    public SqlAbstractParserImpl getParser(Reader stream) {
        return new DaskSqlParserImpl(stream);
    }

}
