package com.ericsson.eniq.events.server.integritytests.ranking.controller;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;

import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

/**
 * 
 * Class responsible only for creating and populating temporary tables for use in certain tests
 * 
 * @author EEMECOY
 *
 */
@Ignore
public class RawTablesPopulatorForMultipleControllerRanking {

    private static final String ERICSSON = "ERICSSON";

    protected static final String BSC1 = "BSC1";

    private static final String GSMCELL1 = "GSMCELL1";

    final static List<String> tempRawTables = new ArrayList<String>();

    protected static final String BSC2 = "BSC2";

    protected static final String RNC1 = "RNC1";

    private static final String RNCCELL1 = "RNCCELL1";

    protected static final String RNC2 = "RNC2";

    private static final int SOME_TAC = 123456;

    private static final String RNCCELL2 = "RNCCELL2";

    private static final String ERBS1 = "ERBS1";

    private static final String LTECELL1 = "LTECELL1";

    private static final String LTECELL2 = "LTECELL2";

    private static final String ERBS2 = "ERBS2";

    private static final String LTECELL3 = "LTECELL3";

    static {

        tempRawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

    }

    private void populateTemporaryTables(final Connection connection) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();
            final String dateTimeNowMinus25 = DateTimeUtilities.getDateTimeMinus25Minutes();

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC2, GSMCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC2, GSMCELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_3G, RNC2, RNCCELL2, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, RAT_INTEGER_VALUE_FOR_3G, RNC2, RNCCELL2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL2, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS2, LTECELL1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS2, LTECELL3, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, sqlExecutor,
                    dateTimeNowMinus25);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1, sqlExecutor,
                    dateTimeNowMinus25);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, sqlExecutor,
                    dateTimeNowMinus25);

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
                            + "(RAT tinyint, VENDOR varchar(128), HIERARCHY_3 varchar(128), HIERARCHY_2 varchar(128), HIERARCHY_1 varchar(128), TAC int, "
                            + "DATETIME_ID timestamp)");

        } finally {
            SQLExecutor.closeSQLExector(sqlExecutor);
        }
    }

    public void createAndPopulateRawTables(final Connection connection) throws SQLException {
        for (final String tempTable : tempRawTables) {
            createTemporaryTable(tempTable, connection);
        }
        populateTemporaryTables(connection);

    }

}
