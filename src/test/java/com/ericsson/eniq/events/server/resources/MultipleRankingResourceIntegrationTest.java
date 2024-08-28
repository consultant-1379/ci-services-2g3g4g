/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static org.junit.Assert.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.json.JSONArray;
import com.ericsson.eniq.events.server.json.JSONException;
import com.ericsson.eniq.events.server.json.JSONObject;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.util.JSONTestUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehaoswa
 * @since  June 2010
 */
public class MultipleRankingResourceIntegrationTest extends BaseServiceIntegrationTest {
    private MultivaluedMap<String, String> map;

    private MultipleRankingService multipleRankingService;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String COUNT = "30";

    private static final String ONE_WEEK = "10080";

    private static final String MAX_ROWS_VALUE = "500";

    @Before
    public void init() {
        multipleRankingService = new MultipleRankingService();
        attachDependencies(multipleRankingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetRankingDataByTimeApnDoesntIncludeNullOrBlankApnsInResult() throws JSONException {
        final String result = testGetRankingDataByTime30Minutes(TYPE_APN);
        assertJSONSucceeds(result);
        final JSONArray dataField = JSONTestUtils.getDataElement(result);
        if (dataField.length() > 0) {
            final JSONObject firstRow = (JSONObject) dataField.get(0);
            final String valueForAPNInRow = (String) firstRow.get("2");
            assertFalse("Value of APN in result should not be an empty string", valueForAPNInRow.equals(""));
        }
    }

    @Test
    public void testInvalidDisplayType() {
        final String invalidDisplayType = "error";

        map.clear();

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        final String result = runQuery(map, multipleRankingService);
        assertNoSuchDisplay(result, invalidDisplayType);

    }

    private String runQuery() {
        return runQueryAndAssertJsonSucceeds(map, multipleRankingService);
    }

    private String testGetRankingDataByTime30Minutes(final String type) {
        return testGetRankingDataByTime(type, "30");
    }

    private String testGetRankingDataByTime(final String type, final String time) {
        map.clear();

        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(COUNT_PARAM, COUNT);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = runQuery();
        return result;

    }

}
