/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * tests for data volume analysis queries
 * @author evarzol
 * @since  July 2011
 */
public class NetworkDataVolumeResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    private MultivaluedMap<String, String> map;

    private NetworkDataVolumeResource networkDataVolumeResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String TIME = "30";

    @Override
    public void onSetUp() {
        networkDataVolumeResource = new NetworkDataVolumeResource();
        attachDependencies(networkDataVolumeResource);
        networkDataVolumeResource.setUriInfo(this.testUri);
        networkDataVolumeResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testDrillDownByAPN() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(NODE_PARAM, "blackberry.net");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = networkDataVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownBySGSN() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    public void testDrillDownByNetwork() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = networkDataVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPNAggregation() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(NODE_PARAM, "blackberry.net");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownBySGSNAggregation() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    public void testDrillDownByNetworkAggregation() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDetailedDataByAPNGroup() throws Exception {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameAPN_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDetailedDataBySGSNGroup() throws Exception {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameEVENTSRC_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetDetailedDataBySGSNGroupWitAggregation() throws Exception {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, "15");
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameEVENTSRC_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testInvalidType() throws Exception {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameEVENTSRC_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = networkDataVolumeResource.getData("requestID", map);
        System.out.println(result);
        assertJSONErrorResult(result);
    }

    private void addTimeParameterToParameterMap() {
        map.putSingle(TIME_QUERY_PARAM, "2440");
    }
}
