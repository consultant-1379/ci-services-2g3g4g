/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.kpiratio;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIByAPNGroupResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.ISRAU;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_TAU;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ISRAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.TAU_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_NET_INIT_DEACTIVATES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_SUCCESSES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_APN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_APN_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_APN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KPIRatioDrillByAPN extends TestsWithTemporaryTablesBaseTestCase<KPIByAPNGroupResult> {

    private KPIRatioResource kpiRatioResource;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
        createAndPopulateAPNGroupTable();
    }

    @Test
    public void testTypeAPNDrilltypeAPNGroup_ISRAU() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateAggregationTablesForSgehQuery(timestamp);
        createAndPopulateRawTablesForSgehQuery(timestamp);
        final String json = runQuery(ISRAU_IN_2G_AND_3G);
        System.out.println(json);
        final List<KPIByAPNGroupResult> summaryResult = getTranslator()
                .translateResult(json, KPIByAPNGroupResult.class);
        validateResultFromSGEHables(summaryResult);

    }

    @Test
    public void testTypeAPNDrilltypeAPNGroup_LTAU() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        createAndPopulateRawTablesForLTEQuery(timestamp);
        populateAggTablesForLTEQuery(timestamp);
        final String json = runQuery(TAU_IN_4G);
        System.out.println(json);
        final List<KPIByAPNGroupResult> summaryResult = getTranslator()
                .translateResult(json, KPIByAPNGroupResult.class);
        validateResultFromLTETables(summaryResult);

    }

    private void createAndPopulateAPNGroupTable() throws Exception {
        final Map<String, Object> columnsAndValues = new HashMap<String, Object>();
        columnsAndValues.put(APN, SAMPLE_APN);
        columnsAndValues.put(GROUP_NAME, SAMPLE_APN_GROUP);
        createTemporaryTable(TEMP_GROUP_TYPE_E_APN, columnsAndValues.keySet());
        insertRow(TEMP_GROUP_TYPE_E_APN, columnsAndValues);
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(APN);
        columnsForAggTable.add(EVENT_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsForAggTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_SUC_DAY, columnsForAggTable);
    }

    private void validateResultFromSGEHables(final List<KPIByAPNGroupResult> results) {
        assertThat(results.size(), is(1));
        final KPIByAPNGroupResult result = results.get(0);
        assertThat(result.getAPNGroup(), is(SAMPLE_APN_GROUP));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(0));
        assertThat(result.getOccurrences(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
    }

    private void populateAggregationTablesForSgehQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_ERR_DAY, values);

    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp) throws SQLException, Exception {

        final Map<String, Object> columnsAndValuesForRawTables = new HashMap<String, Object>();
        columnsAndValuesForRawTables.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTables.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTables.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTables.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTables.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables.keySet());
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables);

    }

    private void createAndPopulateRawTablesForLTEQuery(final String timestamp) throws Exception {

        final Map<String, Object> columnsAndValuesForRawTables = new HashMap<String, Object>();
        columnsAndValuesForRawTables.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTables.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTables.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTables.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTables.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables);

    }

    private void populateAggTablesForLTEQuery(final String timestamp) throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_ERR_DAY, values);
    }

    private String runQuery(final int eventId) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);
        map.putSingle(EVENT_ID_PARAM, Integer.toString(eventId));
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        return kpiRatioResource.getData();
    }

    private void validateResultFromLTETables(final List<KPIByAPNGroupResult> results) {
        assertThat(results.size(), is(1));
        final KPIByAPNGroupResult result = results.get(0);
        assertThat(result.getAPNGroup(), is(SAMPLE_APN_GROUP));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(0));
        assertThat(result.getOccurrences(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
    }
}
