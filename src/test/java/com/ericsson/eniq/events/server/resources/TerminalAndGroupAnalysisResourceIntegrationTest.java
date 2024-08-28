/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehaoswa
 * @since 2010
 *
 */
public class TerminalAndGroupAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    private static final String GROUP_DISPLAY_TYPE = CHART_PARAM;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "14112009";

    private static final String DATE_TO = "17112009";

    private static final String TEST_GROUP_NAME = "someTestGroup-TAC";

    private static final String DRILLTYPE = null;

    @Override
    public void onSetUp() {
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        techPackCXCMappingService.readTechPackLicenseNumbersFromDB();
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostPopularData()}.
     * @throws Exception 
     */
    @Test
    public void testGetGroupMostPopularDataByTimerange() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;
        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTerminalMostPopularDataByTimerange() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_POPULAR;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularDataDrilldownByTimeRange() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularDataDrilldownByTime_FiveMinutes() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostPopularEventSummaryData()}.
     */
    @Test
    public void testGetGroupMostPopularEventSummaryDataDrilldown_OneDay() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(GROUP_NAME_PARAM, "TAC_group");
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostPopularEventSummaryData()}.
     */
    @Test
    public void testGetGroupMostPopularEventSummaryDataDrilldown_FiveMinutes() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, "TAC_group");
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostAttachedFailuresData()}.
     */
    @Test
    public void testGetTerminalMostAttachedFailuresDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostAttachedFailuresData()}.
     */
    @Test
    public void testGetGroupMostAttachedFailuresDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostAttachedFailuresData()}.
     */
    @Test
    public void testGetGroupMostAttachedFailuresDataDrilldownByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostAttachedFailuresData()}.
     */
    @Test
    public void testGetGroupMostAttachedFailuresDataDrilldownByTime_FiveMinutes() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostPDPSessionSetupFailuresData()}.
     */
    @Test
    public void testGetGroupMostPDPSessionSetupFailuresDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_PDP_SESSION_SETUP_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                PDP_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource#getMostMobilityIssuesData()}.
     */
    @Test
    public void testGetGroupMostMobilityIssuesDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_MOBILITY_ISSUES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                MOBILITY_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testNoSuchDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_MOBILITY_ISSUES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                MOBILITY_KEY_VALUE, uriPath);
        assertJSONErrorResult(result);
        assertResultContains(result, E_NO_SUCH_DISPLAY_TYPE);
    }

    @Test
    public void testGetGroupHighestDavavolDrilldownDataByTime() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + HIGHEST_DATAVOL;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetHighestDavavolDataByTimeByTerminalType() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        final String uriPath = TERMINAL_ANALYSIS + "/" + HIGHEST_DATAVOL;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map,
                null, uriPath);
        assertJSONSucceeds(result);
    }

}
