/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyHttpHeaders;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 *
 */
public class CauseCodeTablesCCResourceIntegrationTest extends DataServiceBaseTestCase {

    private CauseCodeTablesCCResource objToTest;

    private MultivaluedMap<String, String> map;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS = "maxRows";

    private static final String MAX_ROWS_VALUE = "50";

    @Override
    public void onSetUp() {
        objToTest = new CauseCodeTablesCCResource();
        attachDependencies(objToTest);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetDataAsJSON() {
        objToTest.setHttpHeaders(new DummyHttpHeaders());
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TZ_OFFSET, "+0000");
        final String resultInJSONFormat = objToTest.getData("requestID", map);
        assertJSONSucceeds(resultInJSONFormat);
    }
}
