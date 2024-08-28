/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehaoswa
 *
 */
public class EventVolumeResourceDisplayTypesIntegrationTest extends DataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private EventVolumeResource eventVolumeResource;

    private static final String TIME = "30";

    @Override
    public void onSetUp() {
        eventVolumeResource = new EventVolumeResource();
        attachDependencies(eventVolumeResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testInvalidDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = eventVolumeResource.getData("requestID", map);
        assertNoSuchDisplay(result, invalidDisplayType);
    }

}
