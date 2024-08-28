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
 * @author evarzol
 * @since July 2011
 */
public class DataVolumeRoamingAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private DatavolRoamingAnalysisResource datavolRoamingAnalysisResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME = "30";

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "01072011";

    private static final String DATE_TO = "08072011";

    @Override
    public void onSetUp() {
        datavolRoamingAnalysisResource = new DatavolRoamingAnalysisResource();
        datavolRoamingAnalysisResource.setTechPackCXCMappingService(techPackCXCMappingService);
        attachDependencies(datavolRoamingAnalysisResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetRoamingDataByCountryByTime() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperatorByTime() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountryByTimerange() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperatorByTimerange() throws Exception {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }
}
