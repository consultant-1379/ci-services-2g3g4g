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
import com.ericsson.eniq.events.server.test.queryresults.KPIByCellFromBSCResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.BSC_PARAM;
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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAT_INTEGER_VALUE_FOR_2G;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAT_INTEGER_VALUE_FOR_4G;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.RAT_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_BSC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM_UPPER_CASE;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ISRAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.TAU_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_NET_INIT_DEACTIVATES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_SUCCESSES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_LTE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BSC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BSC_CELL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_ERBS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_ERBS_CELL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_DAY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KPIRatioDrillByBSC extends TestsWithTemporaryTablesBaseTestCase<KPIByCellFromBSCResult> {

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
    public void testTypeBSCDrilltypeBSC_ISRAU() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateAggregationTablesForSgehQuery(timestamp);
        createAndPopulateRawTablesForSgehQuery(timestamp);
        final String json = runQuery(SAMPLE_BSC, ISRAU_IN_2G_AND_3G, RAT_INTEGER_VALUE_FOR_2G, TWO_WEEKS);
        System.out.println(json);
        final List<KPIByCellFromBSCResult> summaryResult = getTranslator().translateResult(json,
                KPIByCellFromBSCResult.class);
        validateResultFromSGEHables(summaryResult);

    }

    @Test
    public void testTypeBSCDrilltypeBSC_LTAU() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        createAndPopulateRawTablesForLTEQuery(timestamp);
        populateAggTablesForLTEQuery(timestamp);
        final String json = runQuery(SAMPLE_ERBS, TAU_IN_4G, RAT_INTEGER_VALUE_FOR_4G, TWO_WEEKS);
        System.out.println(json);
        final List<KPIByCellFromBSCResult> summaryResult = getTranslator().translateResult(json,
                KPIByCellFromBSCResult.class);
        validateResultFromLTETables(summaryResult);

    }

    @Test
    public void testTypeBSCDrilTypeBSC_LTAUWithDataTieringOn30Min() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus25Minutes();
        createAndPopulateRawTablesForLTEQuery(timestamp);
        populateAggTablesForLTEQuery(timestamp);
        final String json = runQuery(SAMPLE_ERBS, TAU_IN_4G, RAT_INTEGER_VALUE_FOR_4G, THIRTY_MINUTES);
        System.out.println(json);
        final List<KPIByCellFromBSCResult> summaryResult = getTranslator().translateResult(json,
                KPIByCellFromBSCResult.class);
        validateResultFromLTETables(summaryResult);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private String runQuery(final String controller, final int eventId, final String rat, final String time)
            throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(BSC_PARAM, controller);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(EVENT_ID_PARAM, Integer.toString(eventId));
        map.putSingle(RAT_PARAM, rat);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        return kpiRatioResource.getData();
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(RAT);
        columnsForAggTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForAggTable.add(HIERARCHY_1);
        columnsForAggTable.add(HIERARCHY_3);
        columnsForAggTable.add(EVENT_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsForAggTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, columnsForAggTable);
    }

    private void validateResultFromSGEHables(final List<KPIByCellFromBSCResult> results) {
        assertThat(results.size(), is(1));
        final KPIByCellFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getCell(), is(SAMPLE_BSC_CELL));
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
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_1, SAMPLE_BSC_CELL);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_DAY, values);

    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp) throws SQLException, Exception {

        final Map<String, Object> columnsAndValuesForRawTables = new HashMap<String, Object>();
        columnsAndValuesForRawTables.put(HIERARCHY_1, SAMPLE_BSC_CELL);
        columnsAndValuesForRawTables.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTables.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTables.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTables.put(RAT, RAT_FOR_GSM);
        columnsAndValuesForRawTables.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTables.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTables.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables.keySet());
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables);

    }

    private void createAndPopulateRawTablesForLTEQuery(final String timestamp) throws Exception {

        final Map<String, Object> columnsAndValuesForRawTables = new HashMap<String, Object>();
        columnsAndValuesForRawTables.put(HIERARCHY_1, SAMPLE_ERBS_CELL);
        columnsAndValuesForRawTables.put(HIERARCHY_3, SAMPLE_ERBS);
        columnsAndValuesForRawTables.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTables.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTables.put(RAT, RAT_FOR_LTE);
        columnsAndValuesForRawTables.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTables.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTables.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables);

    }

    private void populateAggTablesForLTEQuery(final String timestamp) throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_1, SAMPLE_ERBS_CELL);
        values.put(HIERARCHY_3, SAMPLE_ERBS);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY, values);
    }

    private void validateResultFromLTETables(final List<KPIByCellFromBSCResult> results) {
        assertThat(results.size(), is(1));
        final KPIByCellFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_ERBS));
        assertThat(result.getCell(), is(SAMPLE_ERBS_CELL));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(0));
        assertThat(result.getOccurrences(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
    }
}
