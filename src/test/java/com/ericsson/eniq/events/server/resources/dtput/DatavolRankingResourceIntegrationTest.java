/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehaoswa
 * @since 2011
 *
 */
public class DatavolRankingResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    private MultivaluedMap<String, String> map;

    private DatavolRankingResource datavolRankingResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    @Override
    public void onSetUp() {
        datavolRankingResource = new DatavolRankingResource();
        attachDependencies(datavolRankingResource);
        datavolRankingResource.setUriInfo(this.testUri);
        datavolRankingResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testDataVolumeRankingByIMSI_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingBySGSN_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingBySGSN_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingBySGSN_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_GGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_GGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_GGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByQOS_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_QOS);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByQOS_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_QOS);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByQOS_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_QOS);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData("requestID", map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingBySGSNGroup_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingBySGSNGroup_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingBySGSNGroup_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_RAW() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_15MINS() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_DAY() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults("requestID", map,
                DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

}
