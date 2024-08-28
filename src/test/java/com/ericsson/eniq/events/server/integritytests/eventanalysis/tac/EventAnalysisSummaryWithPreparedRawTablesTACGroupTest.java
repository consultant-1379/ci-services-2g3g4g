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
import java.util.Collections;
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

public class EventAnalysisSummaryWithPreparedRawTablesTACGroupTest extends
        BaseDataIntegrityTest<TACGroupEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private static Collection<String> columnsForGSNRawTable = new ArrayList<String>();

    private final static List<String> rawTables = new ArrayList<String>();

    private final static List<String> gsnRawTables = new ArrayList<String>();

    static {

        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        gsnRawTables.add(TEMP_EVENT_E_GSN_DT_RAW);
        gsnRawTables.add(TEMP_EVENT_E_GSN_DTPDP_RAW);

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
        for (final String rawTable : rawTables) {
            createTemporaryTable(rawTable, columnsForRawTable);
        }
        for (final String gsnRawTable : gsnRawTables) {
            createTemporaryTable(gsnRawTable, columnsForGSNRawTable);
        }
        populateGroupTable();
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void populateGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SONY_ERICSSON_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);

    }

    @Test
    public void testGetSummaryDataWhereJustSuccessEventsExist_TAC_30Minutes() throws Exception {
        populateTemporaryTablesWithOnlySuccessEvents();
        final String json = getData(SONY_ERICSSON_TAC_GROUP);
        validateSuccessEventsAreReturned(json);
    }

    private void validateSuccessEventsAreReturned(final String json) throws Exception {
        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final TACGroupEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(firstResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(firstResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(firstResult.getErrorCount(), is((0)));
        assertThat(firstResult.getSuccessCount(), is((2)));
        assertThat(firstResult.getOccurrences(), is((2)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((0)));

    }

    private String getData(final String groupname) throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, groupname);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void populateTemporaryTablesWithOnlySuccessEvents() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -35);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345678, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DT_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DTPDP_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);
    }

    @Test
    public void testGetSummaryDataForFullDataSet_TAC_30Minutes() throws Exception {
        populateTemporaryTablesWithFullDataSet();
        final String json = getData(SONY_ERICSSON_TAC_GROUP);
        validateResultForFullDataSet(json, SONY_ERICSSON_TAC_GROUP);
    }

    private void validateResultForFullDataSet(final String json, final String groupname) throws Exception {

        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        //sort the result set by eventID (see implementation of compareTo method in BaseEventAnalysisSummaryResultSortableByEventID
        Collections.sort(summaryResult);

        final TACGroupEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTACGroup(), is(groupname));
        assertThat(firstResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(firstResult.getErrorCount(), is((1)));
        assertThat(firstResult.getSuccessCount(), is((1)));
        assertThat(firstResult.getOccurrences(), is((2)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));

        final TACGroupEventAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getTACGroup(), is(groupname));
        assertThat(secondResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(secondResult.getErrorCount(), is((1)));
        assertThat(secondResult.getSuccessCount(), is((2)));
        assertThat(secondResult.getOccurrences(), is((3)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));

        final TACGroupEventAnalysisSummaryResult thirdResult = summaryResult.get(2);
        assertThat(thirdResult.getTACGroup(), is(groupname));
        assertThat(thirdResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((0)));
        assertThat(thirdResult.getOccurrences(), is((1)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final TACGroupEventAnalysisSummaryResult fourthResult = summaryResult.get(3);
        assertThat(fourthResult.getTACGroup(), is(groupname));
        assertThat(fourthResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(fourthResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(fourthResult.getErrorCount(), is((1)));
        assertThat(fourthResult.getSuccessCount(), is((1)));
        assertThat(fourthResult.getOccurrences(), is((2)));
        assertThat(fourthResult.getSuccessRatio(), is(fourthResult.getExpectedSuccessRatio()));
        assertThat(fourthResult.getErrorSubscriberCount(), is((1)));

    }

    private void populateTemporaryTablesWithFullDataSet() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -35);

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

        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DT_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);
        insertRowIntoGSNRawTable(TEMP_EVENT_E_GSN_DTPDP_RAW, SONY_ERICSSON_TAC, 1234567, dateTime);

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

    @Test
    public void testGetSummaryData_TAC_30Minutes_OnlyTACInExTACGroup() throws Exception {
        populateTemporaryTablesWithFullDataSet();
        //add our only TAC to the EXCLUSIVE_TAC group - there should be no data returned.
        populateGroupTableWithExTACGroup();
        final String json = getData(SONY_ERICSSON_TAC_GROUP);
        validateResultIsEmpty(json);
    }

    /**
     * When the chosen group is the EXCLUSIVE_TAC group then event analysis data should be returned
     * @throws Exception
     */
    @Test
    public void testGetSummaryData_TAC_30Minutes_GroupIsExTACGroup() throws Exception {
        populateTemporaryTablesWithFullDataSet();
        //add our only TAC to the EXCLUSIVE_TAC group
        //normally there should be no data returned unless the requested group
        //is the EXCLUSIVE_TAC group (as it is here)
        populateGroupTableWithExTACGroup();
        final String json = getData(EXCLUSIVE_TAC_GROUP);
        validateResultForFullDataSet(json, EXCLUSIVE_TAC_GROUP);
    }

    private void validateResultIsEmpty(final String json) throws Exception {
        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(0));
    }

    private void populateGroupTableWithExTACGroup() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

}
