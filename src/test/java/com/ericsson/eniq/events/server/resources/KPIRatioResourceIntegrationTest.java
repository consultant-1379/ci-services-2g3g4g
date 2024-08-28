/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehaoswa
 * @since 2010
 *
 */
public class KPIRatioResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String MAX_ROWS_VALUE = "50";

    private MultivaluedMap<String, String> map;

    private KPIRatioResource kpiRatioResource;

    @Override
    public void onSetUp() {
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testTypeAPNDrilltypeAPN_30Minutes() {
        runAPNDrilltypeAPN(THIRTY_MINUTES);
    }

    @Test
    public void testTypeAPNDrilltypeAPN_OneWeek() {
        runAPNDrilltypeAPN(ONE_WEEK);
    }

    private void runAPNDrilltypeAPN(final String timePeriod) {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timePeriod);
        map.putSingle(APN_PARAM, "apn1");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeAPNUsingAggregationViews() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(APN_PARAM, "apn1");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeAPNGroup() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, "apnGroup");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeAPNGroupUsingAggregationViews() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(GROUP_NAME_PARAM, "apnGroup");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeSGSNOneWeek() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(APN_PARAM, "apn1");
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeBSC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(APN_PARAM, "apn1");
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeCELL() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(APN_PARAM, "blackberry.net");
        map.putSingle(SGSN_PARAM, "SGSN1");
        map.putSingle(BSC_PARAM, "BSC576");
        map.putSingle(VENDOR_PARAM, "ERICSSON");
        map.putSingle(CELL_PARAM, "CELL115082");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeAPNDrilltypeEVENTS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(APN_PARAM, "apn1");
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(CELL_PARAM, "fda");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TYPE_CAUSE_CODE, "2");
        map.putSingle(TYPE_SUB_CAUSE_CODE, "3");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeSGSNDrilltypeSGSN_30Minutes() {
        runTypeSGSNDrilltypeSGSN(THIRTY_MINUTES);
    }

    @Test
    public void testTypeSGSNDrilltypeSGSN_OneWeek() {
        runTypeSGSNDrilltypeSGSN(ONE_WEEK);
    }

    private void runTypeSGSNDrilltypeSGSN(final String timePeriod) {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timePeriod);
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeSGSNDrilltypeBSC_30Minutes() {
        runQueryTypeSGSNDrilltypeBSC(THIRTY_MINUTES);
    }

    @Test
    public void testTypeSGSNDrilltypeBSC_OneWeek() {
        runQueryTypeSGSNDrilltypeBSC(ONE_WEEK);
    }

    private void runQueryTypeSGSNDrilltypeBSC(final String timePeriod) {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timePeriod);
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeSGSNDrilltypeCELL() throws Exception {

        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(CELL_PARAM, "fda");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);

    }

    @Test
    public void testTypeSGSNDrilltypeEVENTS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(SGSN_PARAM, "sgsn1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(CELL_PARAM, "cell1");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TYPE_CAUSE_CODE, "2");
        map.putSingle(TYPE_SUB_CAUSE_CODE, "3");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrilltypeBSC_ThirtyMinutes() {
        runQueryTypeBSCDrillTypeBSC(THIRTY_MINUTES);
    }

    @Test
    public void testTypeBSCDrilltypeBSC_OneWeek() {
        runQueryTypeBSCDrillTypeBSC(ONE_WEEK);
    }

    private void runQueryTypeBSCDrillTypeBSC(final String timePeriod) {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, timePeriod);
        map.putSingle(BSC_PARAM, "bsc1");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrilltypeCELL() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(CELL_PARAM, "cell1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeBSCDrilltypeEVENTS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(CELL_PARAM, "cell1");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TYPE_CAUSE_CODE, "2");
        map.putSingle(TYPE_SUB_CAUSE_CODE, "3");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrilltypeCELL() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(CELL_PARAM, "cell1");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testTypeCELLDrilltypeEVENTS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(BSC_PARAM, "bsc2");
        map.putSingle(CELL_PARAM, "cell1");
        map.putSingle(VENDOR_PARAM, "vde");
        map.putSingle(EVENT_ID_PARAM, "4");
        map.putSingle(TYPE_CAUSE_CODE, "2");
        map.putSingle(TYPE_SUB_CAUSE_CODE, "3");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = kpiRatioResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

}
