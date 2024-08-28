package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisResourceGroupIntegrationTest extends BaseServiceIntegrationTest {

    private static final String THIRTY_MINUTES = "30";

    private static final String IMSI_GROUP = "myImsiGroup";

    private static final String TAC_GROUP = "myTacGroup";

    private EventAnalysisService eventAnalysisResource;

    private MultivaluedMapImpl map;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    @Before
    public void init() {
        eventAnalysisResource = new EventAnalysisService();
        attachDependencies(eventAnalysisResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testEventAnalysisSummaryForCellGroupKPIRatioDrilldown() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, "someGroupName");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(EVENT_ID_PARAM, "1");
        final String result = getData(eventAnalysisResource, map);
        assertJSONSucceeds(result);
    }

    private String getData(final GenericService service, final MultivaluedMapImpl map1) {
        return runQueryAndAssertJsonSucceeds(map1, eventAnalysisResource);
    }

    @Test
    public void testGetDrillDownDataByImsiGroupAndTimeOfOneWeek() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);

        map.putSingle(GROUP_NAME_PARAM, "imsiEventAnalysisGroup-1");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        final String result = getData(eventAnalysisResource, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDetailedDataByTACGroup() {
        map.clear();

        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, TAC_GROUP);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_ID_PARAM, "1");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testEventAnalysisSummaryForTacGroup30Minutes() {
        map.clear();
        setTimeParamTo30Minutes();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TAC_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);
        assertJSONSucceeds(result);
    }

    private void setTimeParamTo30Minutes() {
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
    }

    @Test
    public void testEventAnalysisSummaryForTacGroup() {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TAC_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testEventAnalysisDetailedForTacGroup() {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TAC_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);
        assertJSONSucceeds(result);
        System.out.println(result);
    }

    @Test
    public void testGetSummaryDataByAPNGroup30Minutes() {
        map.clear();
        setTimeParamTo30Minutes();
        map.putSingle(GROUP_NAME_PARAM, "myApnGroup");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByAPNGroup() {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "myApnGroup");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDetailedDataByAPNGroup() {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "myApnGroup");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_ID_PARAM, "1");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByIMSIGroup30Minutes() {
        map.clear();
        setTimeParamTo30Minutes();
        map.putSingle(GROUP_NAME_PARAM, IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSummaryDataByIMSIGroup() {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDetailedDataByIMSIGroup() {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_ID_PARAM, "1");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    private void addTimeParameterToParameterMap() {
        //map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TIME_QUERY_PARAM, "1440");
    }

    //time=720&display=grid&tzOffset=+0100&maxRows=500&groupname=DG_GroupNameAPN_1&key=SUM&eventID=4&type=APN
    @Test
    public void testGetSummaryDataByAPNGroupWithEventID() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "720");
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameAPN_1");
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, "APN");
        map.putSingle(KEY_PARAM, "SUM");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(MAX_ROWS, "500");
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    //time=30&display=grid&tzOffset=+0100&maxRows=500&groupname=DG_GroupNameEVENTSRC_250&key=SUM&eventID=0&type=SGSN
    @Test
    public void testGetSummaryDataBySGSNGroupWithEventID() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameEVENTSRC_250");
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, "SGSN");
        map.putSingle(KEY_PARAM, "SUM");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(EVENT_ID_PARAM, "0");
        map.putSingle(MAX_ROWS, "500");
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    //time=1440&display=grid&tzOffset=+0100&maxRows=500&key=ERR&type=BSC&RAT=1&vendor=ERICSSON&bsc=ONRM_RootMo_R:RNC02:RNC02&eventID=15
    @Test
    public void testGetErrorDataByBSCWithEventID() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(KEY_PARAM, "ERR");
        map.putSingle(TYPE_PARAM, "BSC");
        map.putSingle(RAT, "1");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(BSC_PARAM, "ONRM_RootMo_R:RNC02:RNC02");
        map.putSingle(EVENT_ID_PARAM, "15");
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }

    //time=30&type=IMSI&imsi=460010000000064&display=grid&key=TOTAL&tzOffset=+0100&maxRows=500 
    @Test
    public void testGetTotalDataByIMSI() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "60");
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(KEY_PARAM, "TOTAL");
        map.putSingle(TYPE_PARAM, "IMSI");
        map.putSingle(IMSI_PARAM, "460010000000064");
        final String result = getData(eventAnalysisResource, map);

        assertJSONSucceeds(result);
    }
}
