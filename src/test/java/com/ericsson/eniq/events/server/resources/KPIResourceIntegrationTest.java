/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.net.URISyntaxException;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehaoswa
 * @since May 2010
 */
public class KPIResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String TIME_VALUE_ONE_DAY = "1440";

    private static final String TIME_VALUE_ONE_WEEK = "10080";

    private static final String TIME_VALUE_FOR_30MINS = "30";

    private MultivaluedMap<String, String> map;

    private KPIResource kpiResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME = "30";

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "11052010";

    private static final String DATE_TO = "12052010";

    private static final String NODE = "BSC193,ERICSSON,1";

    private static final String TIME_VALUE_FIVE_MINUTES = "5";

    private static final String MAX_ROWS_VALUE = "50";

    @Override
    public void onSetUp() {
        kpiResource = new KPIResource();
        attachDependencies(kpiResource);
        kpiResource.lteQueryBuilder = this.lteQueryBuilder;
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetDataByTime_Manufacturer_5Minutes() throws URISyntaxException {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDataByTime_Manufacturer_30Minutes() throws URISyntaxException {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FOR_30MINS);
        map.putSingle("manufacturer", "Shenzhen%20Telsda%20Mobile%20Communication%20Industry%20Devt.Co%20Ltd,");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDataByTime_Manufacturer_1Day() throws URISyntaxException {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDataByTime_Manufacturer_1Week() throws URISyntaxException {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_WEEK);
        map.putSingle("manufacturer", "Ningbo%20Bird%20Co%20Ltd%20%2899%20Chenhshan%20/999%20Dachen%29");
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TAC_5_minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(TAC_PARAM, "123");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TAC_1_day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(TAC_PARAM, "123");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_TAC_1_week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FOR_30MINS);
        map.putSingle(TAC_PARAM, "123");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_5_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FIVE_MINUTES);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_30_Minutes() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_1_Day() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_ONE_DAY);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_BSC_1_Week() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_FOR_30MINS);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_APN() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, "apn1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_SGSN() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetKPIDataByTime_CELL() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(NODE_PARAM, "CELL3000,,BSC1,ERICSSON,1");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    //    @Ignore("4G KPI's do not support groups yet")
    //    public void testGetKPIGroupDataByTime_TAC() throws Exception {
    //        map.clear();
    //        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
    //        map.putSingle(TYPE_PARAM, TYPE_TAC);
    //        map.putSingle(TIME_QUERY_PARAM, TIME);
    //        map.putSingle(GROUP_NAME_PARAM, "testGroup");
    //        map.putSingle(TZ_OFFSET, "+0100");
    //        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
    //        final String result = kpiResource.getData(map);
    //        assertJSONSucceeds(result);
    //    }
    //
    //    @Ignore("4G KPI's do not support groups yet")
    //    public void testGetKPIGroupDataByTime_APN() throws Exception {
    //        map.clear();
    //        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
    //        map.putSingle(TYPE_PARAM, TYPE_APN);
    //        map.putSingle(TIME_QUERY_PARAM, TIME);
    //        map.putSingle(GROUP_NAME_PARAM, "testGroup");
    //        map.putSingle(TZ_OFFSET, "+0100");
    //        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
    //        final String result = kpiResource.getData("requestID", map);
    //        assertJSONSucceeds(result);
    //    }

    @Test
    public void verifyDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_NO_SUCH_DISPLAY_TYPE);
    }

    @Test
    public void testMissingDisplayParam() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingTypeParam() throws Exception {
        map.clear();
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingDisplayAndTypeParam() throws Exception {
        map.clear();
        map.putSingle(NODE_PARAM, NODE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testCheckValidBSC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, "dfasdaf");
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        DummyUriInfoImpl.setUriInfo(map, kpiResource);
        final String result = kpiResource.getData("requestID", map);
        assertJSONErrorResult(result);
    }

}
