/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostattachedfailures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;

/**
 * @author eemecoy
 *
 */
public class MostAttachedFailuresTablesPopulator {

    static final int WORST_TAC = 102900;

    static final String MARKETING_NAME_FOR_WORST_TAC = "Ferry";

    static final String MANFACTURER_FOR_WORST_TAC = "Quanta Computer";

    static final int IMSI1 = 123456789;

    static final String WORST_TAC_GROUP = "sampleTacGroup";

    static final String SECOND_WORST_TAC_GROUP = "secondWorstTacGroup";

    static final int SECOND_WORST_TAC = 107800;

    static final String MARKETING_NAME_FOR_SECOND_WORST_TAC = "0247910";

    static final String MANFACTURER_FOR_SECOND_WORST_TAC = "Garmin International";

    protected TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    final static int noErrorsForWorstTac = 5;

    final static int noErrorsForSecondWorstTac = 3;

    private final Connection connection;

    public MostAttachedFailuresTablesPopulator(final Connection connection) {
        this.connection = connection;
    }

    protected void populateTemporaryTablesForLast3Minutes() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus3Minutes();
        populateTemporaryTables(timeStamp);
    }

    protected void populateTemporaryGroupTable() throws SQLException {
        populateTACGroupTable(WORST_TAC_GROUP, WORST_TAC);
        populateTACGroupTable(WORST_TAC_GROUP, SAMPLE_EXCLUSIVE_TAC);

        populateTACGroupTable(SECOND_WORST_TAC_GROUP, SECOND_WORST_TAC);
    }

    private void populateTemporaryTables(final String timeStamp) throws SQLException {
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, WORST_TAC, noErrorsForWorstTac, timeStamp,
                ATTACH_IN_2G_AND_3G);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, WORST_TAC, noErrorsForWorstTac, timeStamp,
                ATTACH_IN_4G);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, WORST_TAC, timeStamp, ATTACH_IN_2G_AND_3G);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, WORST_TAC, timeStamp, ATTACH_IN_4G);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SECOND_WORST_TAC, noErrorsForSecondWorstTac,
                timeStamp, ATTACH_IN_2G_AND_3G);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SECOND_WORST_TAC, timeStamp, ATTACH_IN_2G_AND_3G);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp, ATTACH_IN_4G);
    }

    protected void populateTemporaryTablesForLast48Hours() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateTemporaryTables(timeStamp);
    }

    private void populateTACGroupTable(final String tacGroup, final int tac) throws SQLException {
        final Map<String, Object> valuesForTacGroupTable = new HashMap<String, Object>();
        valuesForTacGroupTable.put(GROUP_NAME, tacGroup);
        valuesForTacGroupTable.put(TAC, tac);
        new SQLCommand(connection).insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTacGroupTable);
    }

    private void insertRowIntoRawTable(final String table, final int tac, final String timeStamp, final int eventId)
            throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(EVENT_ID, eventId);
        valuesForRawTable.put(TAC, tac);
        valuesForRawTable.put(IMSI, IMSI1);
        valuesForRawTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForRawTable);
    }

    private void insertRowIntoAggTable(final String table, final int tac, final int noErrors, final String timeStamp,
            final int eventId) throws SQLException {
        final Map<String, Object> valuesForAggTable = new HashMap<String, Object>();
        valuesForAggTable.put(NO_OF_ERRORS, noErrors);
        valuesForAggTable.put(EVENT_ID, eventId);
        valuesForAggTable.put(TAC, tac);
        valuesForAggTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForAggTable);
    }

    protected void createTemporaryTables() throws Exception {
        final List<String> columnsForTacAggregationTable = new ArrayList<String>();
        columnsForTacAggregationTable.add(NO_OF_ERRORS);
        columnsForTacAggregationTable.add(EVENT_ID);
        columnsForTacAggregationTable.add(TAC);
        columnsForTacAggregationTable.add(DATETIME_ID);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY,
                columnsForTacAggregationTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY,
                columnsForTacAggregationTable);
        final List<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(TAC);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForRawTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForRawTable);
    }

}
