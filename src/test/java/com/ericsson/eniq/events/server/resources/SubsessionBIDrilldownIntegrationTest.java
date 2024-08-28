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

public class SubsessionBIDrilldownIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private SubsessionBIResource subsessionBIResource;

    private static final String TEST_TIME = "10080";

    private static final String TEST_APN = "APN";

    private static final String TEST_DAY = "Monday";

    private static final String TEST_HOUR = "07";

    private static final String TEST_CELL = "CELL,,BSC,ERICSSON,1";

    private static final String TEST_EVENT = "ATTACH,0";

    private static final String TEST_TAC = "35525201";

    private static final String TEST_IMSI = "460030057273444";

    private static final String TEST_TZ_OFFSET = "+0100";

    private static final String TEST_PTMSI = "0908";

    private static final String TEST_MAX_ROWS = "50";

    private static final String TEST_TAU_DRILL = "007007007,L_TAU";

    private static final String TEST_HANDOVER_DRILL = "CELL,L_TAU";

    private static final String TEST_MSISDN = "123456789012345";

    private static final String TEST_GROUP = "testGroup";

    @Override
    public void onSetUp() {

        subsessionBIResource = new SubsessionBIResource();
        attachDependencies(subsessionBIResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetSubBITAUDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(NODE_PARAM, TEST_TAU_DRILL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITAUDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(NODE_PARAM, TEST_TAU_DRILL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITAUDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(NODE_PARAM, TEST_TAU_DRILL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDrilldownByPTMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(PTMSI_PARAM, TEST_PTMSI);
        map.putSingle(NODE_PARAM, TEST_EVENT);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(NODE_PARAM, TEST_EVENT);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(NODE_PARAM, TEST_EVENT);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIFailureDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(NODE_PARAM, TEST_EVENT);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIFailureData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIHandoverDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(NODE_PARAM, TEST_HANDOVER_DRILL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIHandoverDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(NODE_PARAM, TEST_HANDOVER_DRILL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIHandoverDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(NODE_PARAM, TEST_HANDOVER_DRILL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TAC_PARAM, TEST_TAC);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(TAC_PARAM, TEST_TAC);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBITerminalDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(TAC_PARAM, TEST_TAC);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(NODE_PARAM, TEST_CELL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBICellData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(NODE_PARAM, TEST_CELL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBICellData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBICellDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(NODE_PARAM, TEST_CELL);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBICellData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIAPNDrilldownByIMSI() throws Exception {
        System.out.println("Running test testGetSubBIAPNDrilldown() ");
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(APN_PARAM, TEST_APN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIAPNData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIAPNDrilldownByIMSIGroup() throws Exception {
        System.out.println("Running test testGetSubBIAPNDrilldown() ");
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(APN_PARAM, TEST_APN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIAPNData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIAPNDrilldownByMSISDN() throws Exception {
        System.out.println("Running test testGetSubBIAPNDrilldown() ");
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(APN_PARAM, TEST_APN);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIAPNData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(DAY_PARAM, TEST_DAY);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(DAY_PARAM, TEST_DAY);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyDayDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(DAY_PARAM, TEST_DAY);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyDayData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDrilldownByIMSI() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(HOUR_PARAM, TEST_HOUR);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDrilldownByIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP);
        map.putSingle(HOUR_PARAM, TEST_HOUR);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetSubBIBusyHourDrilldownByMSISDN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN);
        map.putSingle(HOUR_PARAM, TEST_HOUR);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIBusyHourData();
        assertJSONSucceeds(result);
    }
}
