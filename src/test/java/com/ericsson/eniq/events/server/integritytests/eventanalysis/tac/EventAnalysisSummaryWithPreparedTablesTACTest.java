package com.ericsson.eniq.events.server.integritytests.eventanalysis.tac;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.TACEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.ACTIVATE;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.KEY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.KEY_TYPE_SUM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_ATTACH;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_SERVICE_REQUEST;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.SERVICE_REQUEST;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ACTIVATE_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.SERVICE_REQUEST_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.SERVICE_REQUEST_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DATAVOL_DL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DATAVOL_UL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEACTIVATION_TRIGGER;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_NET_INIT_DEACTIVATES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_SUCCESSES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_TOTAL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SONY_ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SONY_ERICSSON_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_TERM_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_TERM_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventAnalysisSummaryWithPreparedTablesTACTest extends BaseDataIntegrityTest<TACEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private static Collection<String> columnsForAggregationTable = new ArrayList<String>();

    private static Collection<String> columnsForGSNDTAggregationTable = new ArrayList<String>();

    private static Collection<String> columnsForGSNDTPDPAggregationTable = new ArrayList<String>();

    private final static List<String> aggregationTables = new ArrayList<String>();

    private final static List<String> rawTables = new ArrayList<String>();

    static {
        aggregationTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY);
        aggregationTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY);
        aggregationTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY);
        aggregationTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY);
        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
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
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_TERM_DAY, columnsForGSNDTAggregationTable);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_TERM_DAY, columnsForGSNDTPDPAggregationTable);

    }

    @Test
    public void testGetSummaryDataWithJustSuccessEventsReturnsThose() throws Exception {

        populateTemporaryTablesWithOnlySuccessEventsForOneEventType();
        final String json = getData();
        validateResultContainsOneEvent(json);
    }

    private void validateResultContainsOneEvent(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(SONY_ERICSSON_TAC));
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
    }

    @Test
    public void testGetSummaryData_TAC_1Week() throws Exception {

        populateTemporaryTablesWithFullDataSet();
        final String json = getData();
        validateResultForFullDataSet(json);
    }

    private String getData() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, String.valueOf(SONY_ERICSSON_TAC));
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void validateResultForFullDataSet(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((8)));
        assertThat(firstResult.getSuccessCount(), is((6)));
        assertThat(firstResult.getOccurrences(), is((14)));
        assertThat(firstResult.getSuccessRatio(), is(42.86));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult fourthResult = summaryResult.get(1);
        assertThat(fourthResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(fourthResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(fourthResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(fourthResult.getErrorCount(), is((7)));
        assertThat(fourthResult.getSuccessCount(), is((5)));
        assertThat(fourthResult.getOccurrences(), is((12)));
        assertThat(fourthResult.getSuccessRatio(), is(41.67));
        assertThat(fourthResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult thirdResult = summaryResult.get(2);
        assertThat(thirdResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((7)));
        assertThat(thirdResult.getOccurrences(), is((8)));
        assertThat(thirdResult.getSuccessRatio(), is(87.50));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));


        final TACEventAnalysisSummaryResult secondResult = summaryResult.get(3);
        assertThat(secondResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((2)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((2)));
        assertThat(secondResult.getSuccessRatio(), is(0.00));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));

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

}
