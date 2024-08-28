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
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

/**
 * @author edivkir
 * @since 2011
 *
 */
public class AggregationTablesPopulatorForMultipleCellRanking {

    protected final static List<String> tempTables = new ArrayList<String>();

    private final int noSuccessesInSgehTable = 3;

    private final int noErrorsForThirdWorstCell_GSMCELL2 = 11;

    private final int noErrorsForFourthWorstCell_LTECELL2 = 4;

    private final int noSuccessesInLteTable = 17;

    public static final int noErrorsForWorstCell_GSMCELL1 = 22;

    public static final int noErrorsForSecondWorstCell_LTECELL1 = 15;

    static {
        tempTables.add(TEMP_EVENT_E_SGEH_VEND_HIER321_SUC_DAY);
        tempTables.add(TEMP_EVENT_E_SGEH_VEND_HIER321_ERR_DAY);
        tempTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_DAY);
        tempTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_ERR_DAY);
        tempTables.add(TEMP_EVENT_E_SGEH_VEND_HIER321_SUC_15MIN);
        tempTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_15MIN);
    }

    void createAndPopulateAggTables(final Connection connection) throws SQLException {
        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable, connection);
        }
        populateTemporaryTables(connection);
    }

    private void populateTemporaryTables(final Connection connection) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            final String dateTimeNowMinus25 = DateTimeUtilities.getDateTimeMinus25Minutes();
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER321_SUC_15MIN, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTimeNowMinus25);
            insertRow(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_15MIN, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1,
                    noSuccessesInLteTable, 0, sqlExecutor, dateTimeNowMinus25);

            final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER321_SUC_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER321_SUC_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL2,
                    noSuccessesInSgehTable, 0, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1,
                    noSuccessesInLteTable, 0, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL2,
                    noSuccessesInLteTable, 0, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER321_ERR_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL1, 0,
                    noErrorsForWorstCell_GSMCELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_VEND_HIER321_ERR_DAY, RAT_INTEGER_VALUE_FOR_2G, BSC1, GSMCELL2, 0,
                    noErrorsForThirdWorstCell_GSMCELL2, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_VEND_HIER321_ERR_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL1, 0,
                    noErrorsForSecondWorstCell_LTECELL1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_VEND_HIER321_ERR_DAY, RAT_INTEGER_VALUE_FOR_4G, ERBS1, LTECELL2, 0,
                    noErrorsForFourthWorstCell_LTECELL2, sqlExecutor, dateTime);

        } finally {
            SQLExecutor.closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String name, final String rat, final String controller, final String cell,
            final int numSuccesses, final int numErrors, final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + name + " values(" + rat + ",'" + ERICSSON + "','" + controller
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
