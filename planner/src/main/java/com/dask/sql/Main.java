package com.dask.sql;

import com.dask.sql.application.DaskSqlParserImplFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class Main {

    public static void main(String[] args) {
        final String SQL = "SELECT age, name FROM person DISTRIBUTE_BY age;";
        SqlParser.Config config = SqlParser.config()
                .withParserFactory(new DaskSqlParserImplFactory())
                .withConformance(SqlConformanceEnum.DEFAULT);
        SqlParser parser = SqlParser.create(SQL, config);

        try {
            SqlNode node = parser.parseQuery(SQL);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Failed to parse the query!!!");
        }
    }
}
