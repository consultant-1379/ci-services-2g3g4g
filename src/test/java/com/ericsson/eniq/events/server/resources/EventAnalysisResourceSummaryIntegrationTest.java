/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Tests for summary queries
 * @author ehaoswa
 * @since  Apr 2010
 */

public class EventAnalysisResourceSummaryIntegrationTest extends BaseServiceIntegrationTest {

    static final String TEST_ERBS = "ERBS1";

    static final String HARD_CODED_LTE_CELL = "LTECELL1";

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    private static final String TIME_VALUE_OF_1_WEEK = "10080";

    private MultivaluedMap<String, String> map;

    private EventAnalysisService eventAnalysisService;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String TIME = "20";

    private static final String MAX_ROWS_VALUE = "500";

    private static final String THIRTY_MINUTES = "30";

    private String runQuery() {

        return runQueryAndAssertJsonSucceeds(map, eventAnalysisService);
    }

    @Before
    public void init() {
        eventAnalysisService = new EventAnalysisService();
        attachDependencies(eventAnalysisService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetEventAnalysisSummaryDataByAPN30Minutes() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(APN_PARAM, "w20e.gd");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        add30MinutesTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByAPNWithNodeParam() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(NODE_PARAM, "w20e.gd");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByAPN() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(APN_PARAM, "w20e.gd");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSNWithNodeParam() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSN() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(SGSN_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByTerminal() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(NODE_PARAM, "G908,35460200");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSN30Minutes() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(SGSN_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        add30MinutesTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByBSC2G() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(BSC_PARAM, "BSC1");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(RAT_PARAM, "0");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByERBS() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(BSC_PARAM, "ERBS1");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(RAT_PARAM, "2");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByBSC2G30Minutes() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(BSC_PARAM, "BSC1");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(RAT_PARAM, "0");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        add30MinutesTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByCell2G30Minutes() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(BSC_PARAM, "BSC1");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(CELL_PARAM, "CELL1");
        map.putSingle(RAT_PARAM, "0");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        add30MinutesTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByCell2G() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(BSC_PARAM, "BSC1");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(CELL_PARAM, "CELL1");
        map.putSingle(RAT_PARAM, "0");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryDataByTac30Minutes() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35460200");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        add30MinutesTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    private void add30MinutesTimeParameterToParameterMap() {
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);

    }

    @Test
    public void testGetEventAnalysisSummaryDataByTac() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35460200");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisSummaryByCellFor4GWithCellBSCVendorAndRATParams() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(BSC_PARAM, TEST_ERBS);
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(CELL_PARAM, HARD_CODED_LTE_CELL);
        map.putSingle(RAT_PARAM, "2");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());

    }

    @Test
    public void testGetDrillDownDataBySGSNSum() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "240");
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());

    }

    @Test
    public void testEventAnalysisSummaryForIMSIGroup() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, "imsiGroup");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testEventAnalysisSummaryForApnGroup() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testEventAnalysisSummaryForBSCGroup() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_BSC_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
        ;
    }

    @Test
    public void testEventAnalysisSummaryForCELLGroup() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, "cellGroup2");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testEventAnalysisSummaryForTACGroupOneMinute() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "1");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "tacGroup");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testEventAnalysisSummaryForCELLGroup15Minutes() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "15");
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, "cellGroup2");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testEventAnalysisSummaryForSGSNGroup() {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(GROUP_NAME_PARAM, "someGroupName");
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisDataByBSC() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, "HIERARCHY_3,ERICSSON,3G");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisDataByCell_fifteenMins() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(NODE_PARAM, "CELL1,,BSC1,ERICSSON,GSM");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetEventAnalysisDataSummaryBy2GCell_oneMin() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(NODE_PARAM, "CELL114903,,BSC1,ERICSSON,GSM");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TIME_QUERY_PARAM, "1");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testCheckValidTAC() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(NODE_PARAM, ".,4234234234");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetDrillDownDataByImsiValueSum() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(IMSI_PARAM, "123456789012345");
        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    private void addTimeParameterToParameterMap() {
        //map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TIME_QUERY_PARAM, "1440");
    }

    @Test
    public void testGetDataByManufacturer_OneWeek() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(MAN_PARAM, "LG Electronics");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

    @Test
    public void testGetDataByManufacturer_30Minutes() {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(MAN_PARAM, "LG Electronics");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQuery());
    }

}
