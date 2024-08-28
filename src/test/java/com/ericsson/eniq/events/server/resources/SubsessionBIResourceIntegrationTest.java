/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eavidat
 * @since 2010
 *
 */

public class SubsessionBIResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private SubsessionBIResource subsessionBIResource;

    private static final String TEST_TIME = "10080";

    private static final String TEST_GROUP_NAME = "VIP";

    private static final String TEST_IMSI = "460030057273444";

    private static final String TEST_MSISDN = "123456789012345";

    private static final String TEST_TZ_OFFSET = "+0100";

    private static final String TEST_PTMSI = "0908";

    private static final String TEST_MAX_ROWS = "50";

    @Override
    public void onSetUp() {

        subsessionBIResource = new SubsessionBIResource();

        attachDependencies(subsessionBIResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetSubBIAPNDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIAPNData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIAPNDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIAPNData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBICellData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBICellData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBICellData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    public void testGetSubBIFailureDataByPTMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        map.putSingle(PTMSI_PARAM, TEST_PTMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITAUDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITAUDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITAUDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIHandOverDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIHandOverDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIHandOverDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalDataByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalDataByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalDataByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBISubscriberDetailsData() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBISubscriberDetailsDataPTMSI() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(PTMSI_PARAM, TEST_PTMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String successResult = subsessionBIResource.getSubBISubscriberDetailsData();
        assertJSONSucceeds(successResult);
    }
}
