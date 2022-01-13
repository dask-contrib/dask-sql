package com.dask.sql;

import com.dask.sql.application.RelationalAlgebraGenerator;
import com.dask.sql.application.RelationalAlgebraGeneratorBuilder;
import com.dask.sql.schema.DaskSchema;
import com.dask.sql.schema.DaskTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.sql.SQLException;

public class Main {

    private static final String SQL1 = "SELECT\n" +
            "            CAST(wcs_user_sk AS INTEGER) AS wcs_user_sk,\n" +
            "            CAST(wcs_item_sk AS INTEGER) AS wcs_item_sk,\n" +
            "            (wcs_click_date_sk * 86400 + wcs_click_time_sk) AS tstamp_inSec\n" +
            "        FROM web_clickstreams\n" +
            "        WHERE wcs_item_sk IS NOT NULL\n" +
            "        AND   wcs_user_sk IS NOT NULL\n" +
            "        DISTRIBUTE BY wcs_user_sk";

    private static final String SQL2 = "WITH item_df AS (\n" +
            "            SELECT wcs_user_sk, session_id\n" +
            "            FROM session_df\n" +
            "            WHERE wcs_item_sk = 10001\n" +
            "        )\n" +
            "        SELECT sd.wcs_item_sk as item_sk_1,\n" +
            "            count(sd.wcs_item_sk) as cnt\n" +
            "        FROM session_df sd\n" +
            "        INNER JOIN item_df id\n" +
            "        ON sd.wcs_user_sk = id.wcs_user_sk\n" +
            "        AND sd.session_id = id.session_id\n" +
            "        AND sd.wcs_item_sk <> 10001\n" +
            "        GROUP BY sd.wcs_item_sk\n" +
            "        ORDER BY cnt desc\n" +
            "        LIMIT 30";

    public static void main(String[] args) {
        DaskSchema schema = new DaskSchema("root");

        DaskTable webClicksTable = new DaskTable("web_clickstreams");
        webClicksTable.addColumn("wcs_user_sk", SqlTypeName.INTEGER);
        webClicksTable.addColumn("wcs_item_sk", SqlTypeName.INTEGER);
        webClicksTable.addColumn("wcs_click_date_sk", SqlTypeName.INTEGER);
        webClicksTable.addColumn("wcs_click_time_sk", SqlTypeName.INTEGER);

        DaskTable sessionTable = new DaskTable("session_df");
        sessionTable.addColumn("wcs_item_sk", SqlTypeName.INTEGER);
        sessionTable.addColumn("wcs_user_sk", SqlTypeName.INTEGER);
        sessionTable.addColumn("session_id", SqlTypeName.INTEGER);

        schema.addTable(webClicksTable);
        schema.addTable(sessionTable);

        RelationalAlgebraGeneratorBuilder builder = new RelationalAlgebraGeneratorBuilder("root", true);
        builder.addSchema(schema);

        RelationalAlgebraGenerator generator = null;
        try {
            generator = builder.build();
            SqlNode sqlNode = generator.getSqlNode(SQL2);
            RelNode relNode = generator.getRelationalAlgebra(sqlNode);
            RelNode optimized = generator.getOptimizedRelationalAlgebra(relNode);
            System.out.println("Optimized Relational Algebra: " + optimized);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ValidationException e) {
            e.printStackTrace();
        } catch (SqlParseException e) {
            e.printStackTrace();
        } catch (RelConversionException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
