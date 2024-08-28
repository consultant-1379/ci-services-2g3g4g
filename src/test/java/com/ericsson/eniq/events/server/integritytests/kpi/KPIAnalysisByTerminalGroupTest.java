/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.kpi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.net.URISyntaxException;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class KPIAnalysisByTerminalGroupTest extends BaseKPIAnalysisTest {

    @Test
    public void testGetKPIRawDataWhenOnlyOneSuccessEventExists() throws Exception {
        final String dateTime = populateRawTemporaryTablesWithJustOneSuccessAndOneFailEvent(3);
        addTACtoTACGroup(SAMPLE_TAC, SAMPLE_TAC_GROUP);
        final String json = getData(FIVE_MINUTES, SAMPLE_TAC_GROUP);
        validateRawResultForOneSuccessAndOneFailEvent(json, dateTime);
    }

    @Test
    public void testGetKPIRawDataWithManyEvents() throws Exception {
        final String dateTime = populateRawTemporaryTablesWithManyEvents(4);
        addTACtoTACGroup(SAMPLE_TAC, SAMPLE_TAC_GROUP);
        final String json = getData(FIVE_MINUTES, SAMPLE_TAC_GROUP);
        validateRawResultForForManyEvents(json, dateTime);
    }

    @Test
    public void testGetKPIRawDataWhenManyEventsExistTACExclusion() throws Exception {
        final String dateTime = populateRawTemporaryTablesWithManyEvents(4);
        addTACtoTACGroup(SAMPLE_TAC, SAMPLE_TAC_GROUP);
        addTACtoTACGroup(SAMPLE_TAC, EXCLUSIVE_TAC_GROUP);
        final String json = getData(FIVE_MINUTES, SAMPLE_TAC_GROUP);
        validateRawResultIsAllZerosForForManyEvents(json, dateTime);
    }

    public void testGetKPIRawDataWhenManyEventsExistWhenGroupIsTACExlusiveTacGroup() throws Exception {
        final String dateTime = populateRawTemporaryTablesWithManyEvents(4);
        addTACtoTACGroup(SAMPLE_TAC, EXCLUSIVE_TAC_GROUP);
        final String json = getData(FIVE_MINUTES, EXCLUSIVE_TAC_GROUP);
        validateRawResultForForManyEvents(json, dateTime);
    }

    private String getData(final String time, final String groupname) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, groupname);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "500");

        return runGetDataQuery(kpiResource, map);
    }

}
