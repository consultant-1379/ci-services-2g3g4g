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
 * @since  May 2011
 */
public class DataVolumeAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    private MultivaluedMap<String, String> map;

    private DataVolumeAnalysisResource dataVolumeAnalysisResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    @Override
    public void onSetUp() {
        dataVolumeAnalysisResource = new DataVolumeAnalysisResource();
        attachDependencies(dataVolumeAnalysisResource);
        dataVolumeAnalysisResource.setUriInfo(this.testUri);
        dataVolumeAnalysisResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testDrillDownByTAC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35347103");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(APN_PARAM, "apn1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownBySGSN() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(SGSN_PARAM, "159.107.165.119");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameIMSI_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameAPN_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownBySGSN_GROUP() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameEVENTSRC_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTERMINAL_GROUP() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameTAC_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    private void addTimeParameterToParameterMap() {
        map.putSingle(TIME_QUERY_PARAM, "2440");
    }
}
