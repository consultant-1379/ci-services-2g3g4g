/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.ranking.accessarea;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.ericsson.eniq.events.server.test.sql.SQLExecutor;

/**
 * @author edivkir
 * @since 2011
 *
 */
public class RawTablesPopulatorForMultipleCellRanking {

    private static final String ERICSSON = "ERICSSON";

    private final static List<String> tempTables = new ArrayList<String>();

    private static final int SOME_TAC = 123;

    static {
        tempTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
    }

    void createAndPopulateTemporaryTables(final Connection connection, final String dateTime) throws SQLException {
        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable, connection);
        }
        populateTemporaryTables(connection, dateTime);
    }

    private void populateTemporaryTables(final Connection connection, final String dateTime) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL2, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL2, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS2, LTECELL2, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS2, LTECELL2, sqlExecutor, dateTime);

        } finally {
            SQLExecutor.closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final String rat, final String controller, final String cell,
            final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + rat + ",'" + ERICSSON + "','" + controller
                + "','','" + cell + "'," + SOME_TAC + ",'" + dateTime + "')");
    }

    private void createTemporaryTable(final String tempTableName, final Connection connection) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor
                    .executeUpdate("create local temporary table "
                            + tempTableName
                            + "(RAT tinyint, VENDOR varchar(128), HIERARCHY_3 varchar(128), HIERARCHY_2 varchar(128), HIERARCHY_1 varchar(128), TAC int, DATETIME_ID timestamp)");

        } finally {
            SQLExecutor.closeSQLExector(sqlExecutor);
        }
    }

}
