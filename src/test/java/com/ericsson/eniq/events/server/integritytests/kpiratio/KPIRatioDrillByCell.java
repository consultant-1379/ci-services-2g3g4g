/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.kpiratio;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.LookupTechPackPopulator;
import com.ericsson.eniq.events.server.test.queryresults.KPITypeCellDrillToCellResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.BSC_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.CELL_PARAM;
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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_CELL;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM_UPPER_CASE;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ISRAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.TAU_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_CODE_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEACTIVATION_TRIGGER;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_LTE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BSC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BSC_CELL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_ERBS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_ERBS_CELL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SUBCAUSE_CODE_COLUMN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_LTE_CAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_LTE_SUBCAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_CAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_SUBCAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KPIRatioDrillByCell extends TestsWithTemporaryTablesBaseTestCase<KPITypeCellDrillToCellResult> {

    private KPIRatioResource kpiRatioResource;

    private final int sgehCauseCode = 1;

    private final int sgehSubCauseCode = 10;

    private final int lteCauseCode = 0;

    private final int lteSubCauseCode = 10;

    private final int deactivationTrigger = 3;

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
        final String timestamp = DateTimeUtilities.getDateTimeMinus5Minutes();
        createAndPopulateRawTablesForSgehQuery(timestamp);
        createDimTables();
        final String json = runQuery(SAMPLE_BSC, ISRAU_IN_2G_AND_3G, RAT_INTEGER_VALUE_FOR_2G, SAMPLE_BSC_CELL);
        System.out.println(json);
        final List<KPITypeCellDrillToCellResult> summaryResult = getTranslator().translateResult(json,
                KPITypeCellDrillToCellResult.class);
        validateResultFromSGEHables(summaryResult);
    }

    @Test
    public void testTypeBSCDrilltypeBSC_LTAU() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus5Minutes();
        createAndPopulateRawTablesForLTEQuery(timestamp);
        createDimTables();
        final String json = runQuery(SAMPLE_ERBS, TAU_IN_4G, RAT_INTEGER_VALUE_FOR_4G, SAMPLE_ERBS_CELL);
        System.out.println(json);
        final List<KPITypeCellDrillToCellResult> summaryResult = getTranslator().translateResult(json,
                KPITypeCellDrillToCellResult.class);
        validateResultFromLTETables(summaryResult);
    }

    private String runQuery(final String controller, final int eventId, final String rat, final String cell)
            throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(CELL_PARAM, cell);
        map.putSingle(BSC_PARAM, controller);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(EVENT_ID_PARAM, Integer.toString(eventId));
        map.putSingle(RAT_PARAM, rat);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");

        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        return kpiRatioResource.getData();
    }

    private void validateResultFromSGEHables(final List<KPITypeCellDrillToCellResult> results) {
        assertThat(results.size(), is(1));
        final KPITypeCellDrillToCellResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getCell(), is(SAMPLE_BSC_CELL));
        assertThat(result.getCauseCode(), is(sgehCauseCode));
        assertThat(result.getSubCauseCode(), is(sgehSubCauseCode));
        assertThat(result.getCauseCodeDesc(), is("UnknownCC1"));
        assertThat(result.getSubCauseCodeDesc(), is("Auth failed"));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp) throws SQLException, Exception {

        final Map<String, Object> columnsAndValuesForRawTables = new HashMap<String, Object>();
        columnsAndValuesForRawTables.put(HIERARCHY_1, SAMPLE_BSC_CELL);
        columnsAndValuesForRawTables.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTables.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTables.put(CAUSE_CODE_COLUMN, sgehCauseCode);
        columnsAndValuesForRawTables.put(SUBCAUSE_CODE_COLUMN, sgehSubCauseCode);
        columnsAndValuesForRawTables.put(DEACTIVATION_TRIGGER, deactivationTrigger);
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
        columnsAndValuesForRawTables.put(CAUSE_CODE_COLUMN, lteCauseCode);
        columnsAndValuesForRawTables.put(SUBCAUSE_CODE_COLUMN, lteSubCauseCode);
        columnsAndValuesForRawTables.put(DEACTIVATION_TRIGGER, deactivationTrigger);
        columnsAndValuesForRawTables.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTables.put(RAT, RAT_FOR_LTE);
        columnsAndValuesForRawTables.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTables.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTables.put(DATETIME_ID, timestamp);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables);

    }

    private void validateResultFromLTETables(final List<KPITypeCellDrillToCellResult> results) {
        assertThat(results.size(), is(1));
        final KPITypeCellDrillToCellResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_ERBS));
        assertThat(result.getCell(), is(SAMPLE_ERBS_CELL));
        assertThat(result.getCauseCode(), is(lteCauseCode));
        assertThat(result.getSubCauseCode(), is(lteSubCauseCode));
        assertThat(result.getCauseCodeDesc(), is("text"));
        assertThat(result.getSubCauseCodeDesc(), is("Auth failed"));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
    }

    private void createDimTables() throws Exception {
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_CAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_LTE_CAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_SUBCAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_LTE_SUBCAUSECODE);
    }

}
