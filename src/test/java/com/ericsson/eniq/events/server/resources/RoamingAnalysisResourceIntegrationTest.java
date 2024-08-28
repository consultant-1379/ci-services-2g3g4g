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
 * @since May 2010
 */
public class RoamingAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private RoamingAnalysisResource roamingAnalysisResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME = "30";

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "18012011";

    private static final String DATE_TO = "19012011";

    @Override
    public void onSetUp() {
        roamingAnalysisResource = new RoamingAnalysisResource();
        attachDependencies(roamingAnalysisResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetRoamingDataByCountryLast5Minutes() {
        getRoamingDataByCountry(FIVE_MINUTES);
    }

    private void getRoamingDataByCountry(final String timePeriod) {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, timePeriod);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = roamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountryLast30Minutes() {
        getRoamingDataByCountry(THIRTY_MINUTES);
    }

    @Test
    public void testGetRoamingDataByCountryLast1Day() {
        getRoamingDataByCountry(ONE_DAY);
    }

    @Test
    public void testGetRoamingDataByCountryLastWeek() {
        getRoamingDataByCountry(ONE_WEEK);
    }

    @Test
    public void testGetRoamingDataByOperatorLast5Minutes() {
        getRoamingDataByOperator(FIVE_MINUTES);
    }

    @Test
    public void testGetRoamingDataByOperatorLast30Minutes() {
        getRoamingDataByOperator(THIRTY_MINUTES);
    }

    @Test
    public void testGetRoamingDataByOperatorLast1Day() {
        getRoamingDataByOperator(ONE_DAY);
    }

    @Test
    public void testGetRoamingDataByOperatorLast1Week() {
        getRoamingDataByOperator(ONE_WEEK);
    }

    private void getRoamingDataByOperator(final String timePeriod) {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, timePeriod);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = roamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
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
        final String result = roamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
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
        final String result = roamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void verifyDisplayType() throws Exception {
        final String invalidDisplayType = "error";
        map.clear();

        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = roamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }
}
