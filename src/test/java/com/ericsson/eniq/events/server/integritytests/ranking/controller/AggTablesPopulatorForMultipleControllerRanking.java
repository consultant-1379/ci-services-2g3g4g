package com.ericsson.eniq.events.server.integritytests.ranking.controller;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
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
 * Class responsible only for creating and populating temporary tables for use in other tests
 *
 * @author EEMECOY
 *
 */
@Ignore
public class AggTablesPopulatorForMultipleControllerRanking {

    final static List<String> tempTables = new ArrayList<String>();

    final static int noSuccessesInSgehTable = 3;

    protected final static int noErrorsForWorstBSC_BSC1 = 22;

    protected final static int noErrorsForSecondWorstBSC_BSC2 = 2;

    protected final static int noErrorsForWorstRNC_RNC1 = 5;

    protected final static int noErrorsForSecondWorstRNC_RNC2 = 3;

    final static int noSuccessesInLteTable = 2;

    final static int noErrorsForWorstERBS_ERBS1 = 22;

    final static int noErrorsForSecondWorstERBS_ERBS2 = 17;

    static {
        tempTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_DAY);
        tempTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_ERR_DAY);
        tempTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_SUC_DAY);
        tempTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_ERR_DAY);
        tempTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_15MIN);
        tempTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_SUC_15MIN);

    }

    void createAndPopulateAggTables(final Connection connection) throws SQLException {
        for (final String tempTable : AggTablesPopulatorForMultipleControllerRanking.tempTables) {
            createTemporaryTable(tempTable, connection);
        }
        populateTemporaryTables(connection);
    }

    private void populateTemporaryTables(final Connection connection) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();
            final String dateTimeNowMinus25 = DateTimeUtilities.getDateTimeMinus25Minutes();

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC2, GSMCELL2,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC2, GSMCELL2,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1,
                    noSuccessesInLteTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_SUC_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL2,
                    noSuccessesInLteTable, 0, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, 0,
                    noErrorsForWorstBSC_BSC1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC2, GSMCELL2, 0,
                    noErrorsForSecondWorstBSC_BSC2, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC2, GSMCELL2, 0,
                    noErrorsForSecondWorstBSC_BSC2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1, 0,
                    noErrorsForWorstRNC_RNC1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_3G, RNC2, RNCCELL1, 0,
                    noErrorsForSecondWorstRNC_RNC2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, 0,
                    noErrorsForWorstERBS_ERBS1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_ERR_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS2, LTECELL2, 0,
                    noErrorsForSecondWorstERBS_ERBS2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_15MIN, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTimeNowMinus25);

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_SUC_15MIN, RAT_INTEGER_VALUE_FOR_3G, RNC1, RNCCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTimeNowMinus25);

            insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_SUC_15MIN, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL2,
                    noSuccessesInLteTable, 0, sqlExecutor, dateTimeNowMinus25);

        } finally {
            SQLExecutor.closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final String rat, final String controller, final String cell,
            final int numSuccesses, final int numErrors, final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + rat + ",'" + ERICSSON + "','" + controller
                + "','','" + cell + "'," + numSuccesses + "," + numErrors + ",'" + dateTime + "')");
    }

    private void createTemporaryTable(final String tempTableName, final Connection connection) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor
                    .executeUpdate("create local temporary table "
                            + tempTableName
                            + "(RAT tinyint, VENDOR varchar(128), HIERARCHY_3 varchar(128), HIERARCHY_2 varchar(128), HIERARCHY_1 varchar(128), NO_OF_SUCCESSES int, "
                            + "NO_OF_ERRORS int, DATETIME_ID timestamp)");

        } finally {
            SQLExecutor.closeSQLExector(sqlExecutor);
        }
    }

}
