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

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIByAPNAndSGSNResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.APN_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.ISRAU;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.LTE;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_TAU;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.SGSN_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM_UPPER_CASE;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ISRAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.TAU_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEACTIVATION_TRIGGER;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EVENT_SOURCE_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_NET_INIT_DEACTIVATES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_SUCCESSES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_LTE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_APN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BSC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_ERBS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_MME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_SGSN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KPIRatioDrillBySGSN extends TestsWithTemporaryTablesBaseTestCase<KPIByAPNAndSGSNResult> {

    private KPIRatioResource kpiRatioResource;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_2G3GEventId() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        createAndPopulateRawTablesForSgehQuery(timestamp);
        populateAggregationTablesForSgehQuery(timestamp);
        final String result = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, TWO_WEEKS);
        validateResultForSGEHQuery(result);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_2G3GEventIdWithDataTiering() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus25Minutes();
        createAndPopulateRawTablesForSgehQuery(timestamp);
        populateSuccessAggregationTablesForSgehQuery(timestamp);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, THIRTY_MINUTES);

        final List<KPIByAPNAndSGSNResult> results = getTranslator().translateResult(json, KPIByAPNAndSGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNAndSGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getAPN(), is(SAMPLE_APN));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(1));
        assertThat(result.getOccurrences(), is(2));
        assertThat(result.getNoTotalErrSubscribers(), is(1));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_4GEventId() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        createAndPopulateRawTablesForLteQuery(timestamp);
        populateAggregationTablesForLteQuery(timestamp);
        final String result = runQuery(SAMPLE_MME, TAU_IN_4G, TWO_WEEKS);
        validateResultForLTEQuery(result);
    }

    private String runQuery(final String sgsnOrMME, final int eventId, final String time) throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        final String result = kpiRatioResource.getData();
        System.out.println(result);
        return result;
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(APN);
        columnsForAggTable.add(EVENT_SOURCE_NAME);
        columnsForAggTable.add(RAT);
        columnsForAggTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForAggTable.add(HIERARCHY_3);
        columnsForAggTable.add(EVENT_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsForAggTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, columnsForAggTable);
    }

    private void validateResultForLTEQuery(final String json) throws Exception {
        final List<KPIByAPNAndSGSNResult> results = getTranslator().translateResult(json, KPIByAPNAndSGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNAndSGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getAPN(), is(SAMPLE_APN));
        assertThat(result.getSGSN(), is(SAMPLE_MME));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_ERBS));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(0));
        assertThat(result.getOccurrences(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));

    }

    private void populateAggregationTablesForLteQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_ERBS);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, values);

    }

    private void createAndPopulateRawTablesForLteQuery(final String timestamp) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_ERBS);
        columnsAndValuesForRawTable.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_LTE);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);

    }

    private void validateResultForSGEHQuery(final String json) throws Exception {
        final List<KPIByAPNAndSGSNResult> results = getTranslator().translateResult(json, KPIByAPNAndSGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNAndSGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getAPN(), is(SAMPLE_APN));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is(GSM));
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
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, values);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, values);

    }

    private void populateSuccessAggregationTablesForSgehQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, values);
    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp) throws SQLException, Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTable.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_GSM);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 1);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
    }
}
