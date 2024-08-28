package com.ericsson.eniq.events.server.integritytests.eventanalysis.tac;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.TACGroupEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryWithPreparedTablesTacGroupTest extends
        BaseDataIntegrityTest<TACGroupEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private static Collection<String> columnsForAggregationTable = new ArrayList<String>();

    private static Collection<String> columnsForGSNDTAggregationTable = new ArrayList<String>();

    private static Collection<String> columnsForGSNDTPDPAggregationTable = new ArrayList<String>();

    private final static List<String> aggregationTables = new ArrayList<String>();

    private final static List<String> rawTables = new ArrayList<String>();

    private static Collection<String> columnsForGSNRawTable = new ArrayList<String>();

    private final static List<String> gsnRawTables = new ArrayList<String>();

    static {
        aggregationTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY);
        aggregationTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY);
        aggregationTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY);
        aggregationTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY);

        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        gsnRawTables.add(TEMP_EVENT_E_GSN_DT_RAW);
        gsnRawTables.add(TEMP_EVENT_E_GSN_DTPDP_RAW);

        columnsForAggregationTable.add(MANUFACTURER);
        columnsForAggregationTable.add(EVENT_ID);
        columnsForAggregationTable.add(NO_OF_SUCCESSES);
        columnsForAggregationTable.add(NO_OF_ERRORS);
        columnsForAggregationTable.add(TAC);
        columnsForAggregationTable.add(IMSI);
        columnsForAggregationTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsForAggregationTable.add(DATETIME_ID);

        columnsForGSNDTAggregationTable.add(TAC);
        columnsForGSNDTAggregationTable.add(IMSI);
        columnsForGSNDTAggregationTable.add(DATETIME_ID);
        columnsForGSNDTAggregationTable.add(DATAVOL_UL);
        columnsForGSNDTAggregationTable.add(DATAVOL_DL);

        columnsForGSNDTPDPAggregationTable.add(TAC);
        columnsForGSNDTPDPAggregationTable.add(IMSI);
        columnsForGSNDTPDPAggregationTable.add(DATETIME_ID);
        columnsForGSNDTPDPAggregationTable.add(DATAVOL_UL);
        columnsForGSNDTPDPAggregationTable.add(DATAVOL_DL);
        columnsForGSNDTPDPAggregationTable.add(NO_OF_TOTAL);

        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DEACTIVATION_TRIGGER);
        columnsForRawTable.add(DATETIME_ID);

        columnsForGSNRawTable.add(TAC);
        columnsForGSNRawTable.add(IMSI);
        columnsForGSNRawTable.add(DATETIME_ID);
        columnsForGSNRawTable.add(DATAVOL_UL);
        columnsForGSNRawTable.add(DATAVOL_DL);
    }

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);
        for (final String tempTable : aggregationTables) {
            createTemporaryTable(tempTable, columnsForAggregationTable);
        }
        for (final String ratTable : rawTables) {
            createTemporaryTable(ratTable, columnsForRawTable);
        }
        for (final String gsnRawTable : gsnRawTables) {
            createTemporaryTable(gsnRawTable, columnsForGSNRawTable);
        }
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_TERM_DAY, columnsForGSNDTAggregationTable);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_TERM_DAY, columnsForGSNDTPDPAggregationTable);

        populateGroupTable();
    }

    private void populateGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SONY_ERICSSON_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    @Test
    public void testGetSummaryDataWithJustSuccessEventsReturnsThose() throws Exception {
        populateTemporaryTablesWithOnlySuccessEventsForOneEventType();
        final String json = getData();
        validateResultContainsOneEvent(json);
    }

    private void validateResultContainsOneEvent(final String json) throws Exception {
        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final TACGroupEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(firstResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(firstResult.getErrorCount(), is((0)));
        assertThat(firstResult.getSuccessCount(), is((5)));
        assertThat(firstResult.getOccurrences(), is((5)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((0)));

    }

    private void populateTemporaryTablesWithOnlySuccessEventsForOneEventType() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON,
                ACTIVATE_IN_2G_AND_3G, 5, 0, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoDTAggregationTable(TEMP_EVENT_E_GSN_DT_TERM_DAY, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoDTPDPAggregationTable(TEMP_EVENT_E_GSN_DTPDP_TERM_DAY, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DT_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DTPDP_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);
    }

    @Test
    public void testGetSummaryData_TAC_1Week() throws Exception {

        populateTemporaryTablesWithFullDataSet();
        final String json = getData();
        validateResultForFullDataSet(json);
    }

    private String getData() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, String.valueOf(MINUTES_IN_2_WEEKS));
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, SONY_ERICSSON_TAC_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void validateResultForFullDataSet(final String json) throws Exception {
        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        final TACGroupEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(firstResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((2)));
        assertThat(firstResult.getSuccessCount(), is((0)));
        assertThat(firstResult.getOccurrences(), is((2)));
        assertThat(firstResult.getSuccessRatio(), is(0.00));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));

        final TACGroupEventAnalysisSummaryResult thirdResult = summaryResult.get(2);
        assertThat(thirdResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((7)));
        assertThat(thirdResult.getOccurrences(), is((8)));
        assertThat(thirdResult.getSuccessRatio(), is(87.50));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final TACGroupEventAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(secondResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((8)));
        assertThat(secondResult.getSuccessCount(), is((6)));
        assertThat(secondResult.getOccurrences(), is((14)));
        assertThat(secondResult.getSuccessRatio(), is(42.86));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));

        final TACGroupEventAnalysisSummaryResult fourthResult = summaryResult.get(3);
        assertThat(fourthResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(fourthResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(fourthResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(fourthResult.getErrorCount(), is((7)));
        assertThat(fourthResult.getSuccessCount(), is((5)));
        assertThat(fourthResult.getOccurrences(), is((12)));
        assertThat(fourthResult.getSuccessRatio(), is(41.67));
        assertThat(fourthResult.getErrorSubscriberCount(), is((1)));

    }

    private void populateTemporaryTablesWithFullDataSet() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345671, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345672,
                dateTime);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345674,
                dateTime);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345675, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, 12345676, dateTime);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345678, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON,
                ACTIVATE_IN_2G_AND_3G, 5, 0, SONY_ERICSSON_TAC, 12345671, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON,
                SERVICE_REQUEST_IN_2G_AND_3G, 6, 0, SONY_ERICSSON_TAC, 12345672, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON,
                ACTIVATE_IN_2G_AND_3G, 0, 7, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON,
                SERVICE_REQUEST_IN_2G_AND_3G, 0, 8, SONY_ERICSSON_TAC, 12345674, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 1,
                SONY_ERICSSON_TAC, 12345675, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_4G,
                0, 2, SONY_ERICSSON_TAC, 12345676, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 3, 0,
                SONY_ERICSSON_TAC, 12345677, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 4, 0,
                SONY_ERICSSON_TAC, 12345678, dateTime);

        insertRowIntoDTAggregationTable(TEMP_EVENT_E_GSN_DT_TERM_DAY, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoDTPDPAggregationTable(TEMP_EVENT_E_GSN_DTPDP_TERM_DAY, SONY_ERICSSON_TAC, 12345673, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DT_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DTPDP_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);

    }

    private void insertRowIntoAggregationTable(final String table, final String manufacturer, final int eventId,
            final int successCount, final int errorCount, final int tac, final int imsi, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(MANUFACTURER, manufacturer);
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_SUCCESSES, successCount);
        values.put(NO_OF_ERRORS, errorCount);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 0);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

    private void insertRowIntoDTAggregationTable(final String table, final int tac, final int imsi,
            final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DATAVOL_DL, 500000);
        values.put(DATAVOL_UL, 500000);
        insertRow(table, values);
    }

    private void insertRowIntoDTPDPAggregationTable(final String table, final int tac, final int imsi,
            final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DATAVOL_DL, 500000);
        values.put(DATAVOL_UL, 500000);
        values.put(NO_OF_TOTAL, 30000);
        insertRow(table, values);
    }

    private void insertRowIntoRawTable(final String table, final int eventId, final int tac, final int imsi,
            final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DEACTIVATION_TRIGGER, 1);
        insertRow(table, values);
    }

    private void insertRowIntoGSNRawTable(final String table, final int tac, final int imsi, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DATAVOL_DL, 500000);
        values.put(DATAVOL_UL, 500000);
        insertRow(table, values);
    }

}
