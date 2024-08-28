/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventvolume;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.EventVolumeResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.EventVolumeResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.CHART_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.NODE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ACTIVATE_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.DEACTIVATE_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.DEDICATED_BEARER_ACTIVATE_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.DEDICATED_BEARER_DEACTIVATE_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.DETACH_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.DETACH_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.HANDOVER_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ISRAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.PDN_CONNECT_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.PDN_DISCONNECT_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.RAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.SERVICE_REQUEST_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.SERVICE_REQUEST_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.TAU_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEACTIVATION_TRIGGER;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EVENT_SOURCE_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_SGSN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SONY_ERICSSON_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.VENDOR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public abstract class BaseEventVolumeWithPreparedTablesTest extends
        TestsWithTemporaryTablesBaseTestCase<EventVolumeResult> {

    private final EventVolumeResource eventVolumeResource = new EventVolumeResource();

    private static Collection<String> columnsForRawTables = new ArrayList<String>();

    protected final Collection<String> columnsForAggTables = getColumnsForAggTables();

    protected static Collection<String> groupColumns = new ArrayList<String>();

    protected final static List<String> rawTables = new ArrayList<String>();

    protected final List<String> aggTables = getAggTables();

    protected final static int[] eventIDs = { ATTACH_IN_2G_AND_3G, ACTIVATE_IN_2G_AND_3G, RAU_IN_2G_AND_3G,
            ISRAU_IN_2G_AND_3G, DEACTIVATE_IN_2G_AND_3G, ATTACH_IN_4G, DETACH_IN_4G, HANDOVER_IN_4G, TAU_IN_4G,
            DEDICATED_BEARER_ACTIVATE_IN_4G, DEDICATED_BEARER_DEACTIVATE_IN_4G, PDN_CONNECT_IN_4G,
            PDN_DISCONNECT_IN_4G, SERVICE_REQUEST_IN_4G, DETACH_IN_2G_AND_3G, SERVICE_REQUEST_IN_2G_AND_3G };

    private final static int EXPECTED_TOTAL_RESULTS_PER_EVENT_ID = 4;

    private final static int EXPECTED_ERROR_RESULTS_PER_EVENT_ID = 2;

    protected final static int ONE_SUCCESS_EVENT = 1;

    protected final static int ONE_ERROR_EVENT = 1;

    static {

        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        columnsForRawTables.add(EVENT_ID);
        columnsForRawTables.add(TAC);
        columnsForRawTables.add(EVENT_SOURCE_NAME);
        columnsForRawTables.add(IMSI);
        columnsForRawTables.add(DEACTIVATION_TRIGGER);
        columnsForRawTables.add(DATETIME_ID);
        columnsForRawTables.add(RAT);
        columnsForRawTables.add(VENDOR);
        columnsForRawTables.add(HIERARCHY_1);
        columnsForRawTables.add(HIERARCHY_3);

        groupColumns.add(GROUP_NAME);
        groupColumns.add(TAC);

    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        attachDependencies(eventVolumeResource);
        for (final String rawTable : rawTables) {
            createTemporaryTable(rawTable, columnsForRawTables);
        }
        for (final String aggTable : aggTables) {
            createTemporaryTable(aggTable, columnsForAggTables);
        }
    }

    protected abstract Collection<String> getColumnsForAggTables();

    protected abstract List<String> getAggTables();

    protected String getData(final String time, final String type, final String node) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(NODE_PARAM, node);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);
    }

    protected String getGroupData(final String time, final String type, final String group) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(GROUP_NAME_PARAM, group);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);

    }

    protected void validateResults(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult,
                EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(2)); //2  time intervals

        for (final EventVolumeResult eventVolumeResult : eventVolumeResults) {
            validateSingleResult(eventVolumeResult);
        }
    }

    protected void validateResults_6Hours(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult,
                EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(24)); //24  points = 6* (1 hour divided by 15min)

        for (final EventVolumeResult eventVolumeResult : eventVolumeResults) {
            validateSingleResult(eventVolumeResult);
        }
    }

    protected void validateEmptyResult(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult,
                EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(0)); //2  time intervals
    }

    protected void validateSingleResult(final EventVolumeResult eventVolumeResult) {

        assertThat(eventVolumeResult.getAttachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getAttachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getActivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getActivateEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getRauEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getRauEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getIsrauEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getIsrauEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDeactivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDeactivateEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDetachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDetachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getServiceRequestEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getServiceRequestEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLAttachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLAttachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDetachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDetachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLHandoverEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLHandoverFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLTauEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLTauFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerActivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerActivateFailEventCount(),
                is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerDeactivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerDeactivateFailEventCount(),
                is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnConnectEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnConnectFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnDisconnectEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnDisconnectFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLServiceRequestEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLServiceRequestFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));

    }

    protected void populateTemporaryTablesWithFiveMinDataSet() throws SQLException {
        final String dateTimeMinus2 = DateTimeUtilities.getDateTimeMinus2Minutes();
        final String dateTimeMinus3 = DateTimeUtilities.getDateTimeMinus3Minutes();

        //here I add 2 events - 1 for each of the 2 minutes in question.
        //I do this for each event type, and to each RAW table.
        //This should result in 2 successes and 2 failures per minute. 
        for (final String rawTable : rawTables) {
            for (final int eventID : eventIDs) {
                insertRowIntoRawTable(rawTable, eventID, dateTimeMinus2);
                insertRowIntoRawTable(rawTable, eventID, dateTimeMinus3);
            }
        }
    }

    protected void populateTemporaryTablesWithThirtyMinDataSet() throws SQLException {
        final String dateTimeMinus5 = DateTimeUtilities.getDateTimeMinus25Minutes();
        final String dateTimeMinus25 = DateTimeUtilities.getDateTimeMinus30Minutes();
        populateTemporaryTablesWithAggDataSet(dateTimeMinus5, dateTimeMinus25);
    }

    protected void populateTemporaryTablesWithAggDataSet(final String time1, final String time2) throws SQLException {
        //here I add 2 events - 1 for each of the 2 minutes in question.
        //I do this for each event type, and to each RAW table.
        //This should result in 2 successes and 2 failures per minute. 
        for (final String rawTable : rawTables) {
            for (final int eventID : eventIDs) {
                insertRowIntoRawTable(rawTable, eventID, time1);
                insertRowIntoRawTable(rawTable, eventID, time2);
            }
        }
        for (final String aggTable : aggTables) {
            for (final int eventID : eventIDs) {
                insertRowIntoAggTable(aggTable, eventID, time1);
                insertRowIntoAggTable(aggTable, eventID, time2);
            }
        }
    }

    protected void insertRowIntoRawTable(final String table, final int eventId, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, SONY_ERICSSON_TAC);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(IMSI, SAMPLE_IMSI);
        values.put(DATETIME_ID, dateTime);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_1, "00");
        values.put(HIERARCHY_3, "BSC1");
        values.put(DEACTIVATION_TRIGGER, 1);
        insertRow(table, values);
    }

    protected abstract void insertRowIntoAggTable(final String table, final int eventId, final String dateTime)
            throws SQLException;

    protected void populateGroupTableWithExclusiveTAC() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

}
