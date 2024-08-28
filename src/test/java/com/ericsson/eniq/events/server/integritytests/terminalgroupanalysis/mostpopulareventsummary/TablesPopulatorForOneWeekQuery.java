/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_SUCCESSES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;

/**
 * Populate temporary tables with data required for longer time ranges - ie add data to the aggregation tables, plus
 * the matching data in the raw tables for joins on the raw tables
 * @author eemecoy
 *
 */
public class TablesPopulatorForOneWeekQuery {

    public static final int MOST_POPULAR_TAC = 102000;

    public static final String MANUFACTURER_FOR_MOST_POPULAR_TAC = "RIM";

    public static final String MARKETING_NAME_FOR_MOST_POPULAR_TAC = "RAP40GW";

    public static final String MOST_POPULAR_TAC_GROUP = "mostPopularTacGroup";

    static final int IMSI1 = 123456789;

    public static final int SECOND_MOST_POPULAR_TAC = 100100;

    public static final String MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC = "Mitsubishi";

    public static final String MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC = "G410";

    public final static int noErrorsForMostPopularTacInSgehTable = 4;

    public final static int noSuccessesForMostPopularTac = 2;

    public final static int noErrorsForSecondMostPopularTac = 0;

    public final static int noSuccessesForSecondMostPopularTac = 5;

    public final static int noErrorsForMostPopularTacInLteTable = 7;

    public final static String SECOND_MOST_POPULAR_TAC_GROUP = "secondMostPopularTacGroup";

    public static final int THIRD_MOST_POPULAR_TAC = 789789;

    public static final String THIRD_MOST_POPULAR_TAC_GROUP = "thirdMostPopularTacGroup";

    public final static int noErrorsForThirdPopularTacInLteTable = 4;

    private final Connection connection;

    public TablesPopulatorForOneWeekQuery(final Connection connection) {
        this.connection = connection;
    }

    public void createTemporaryTables() throws Exception {
        final List<String> columnsForTacAggregationTable = new ArrayList<String>();
        columnsForTacAggregationTable.add(NO_OF_SUCCESSES);
        columnsForTacAggregationTable.add(NO_OF_ERRORS);
        columnsForTacAggregationTable.add(TAC);
        columnsForTacAggregationTable.add(DATETIME_ID);
        final SQLCommand sqlCommand = new SQLCommand(connection);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, columnsForTacAggregationTable);

        final List<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(TAC);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsForRawTable);

    }

    public void populateTemporaryTables() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY, MOST_POPULAR_TAC,
                noErrorsForMostPopularTacInSgehTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTac,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, MOST_POPULAR_TAC,
                noErrorsForMostPopularTacInLteTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTac,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, SECOND_MOST_POPULAR_TAC,
                noErrorsForSecondMostPopularTac, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, SECOND_MOST_POPULAR_TAC, 0,
                noSuccessesForSecondMostPopularTac, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, THIRD_MOST_POPULAR_TAC,
                noErrorsForThirdPopularTacInLteTable, 0, timeStamp);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);

        populateTACGroupTable(MOST_POPULAR_TAC_GROUP, MOST_POPULAR_TAC);
        populateTACGroupTable(MOST_POPULAR_TAC_GROUP, SAMPLE_EXCLUSIVE_TAC);
        populateTACGroupTable(SECOND_MOST_POPULAR_TAC_GROUP, SECOND_MOST_POPULAR_TAC);
        populateTACGroupTable(THIRD_MOST_POPULAR_TAC_GROUP, THIRD_MOST_POPULAR_TAC);
    }

    private void populateTACGroupTable(final String tacGroup, final int tac) throws SQLException {
        final Map<String, Object> valuesForTacGroupTable = new HashMap<String, Object>();
        valuesForTacGroupTable.put(GROUP_NAME, tacGroup);
        valuesForTacGroupTable.put(TAC, tac);
        new SQLCommand(connection).insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTacGroupTable);
    }

    private void insertRowIntoRawTable(final String table, final int tac, final String timeStamp) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(TAC, tac);
        valuesForRawTable.put(IMSI, IMSI1);
        valuesForRawTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForRawTable);
    }

    private void insertRowIntoAggTable(final String table, final int tac, final int noErrors, final int noSuccesses,
            final String timeStamp) throws SQLException {
        final Map<String, Object> valuesForAggTable = new HashMap<String, Object>();
        valuesForAggTable.put(NO_OF_ERRORS, noErrors);
        valuesForAggTable.put(NO_OF_SUCCESSES, noSuccesses);
        valuesForAggTable.put(TAC, tac);
        valuesForAggTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForAggTable);
    }
}
