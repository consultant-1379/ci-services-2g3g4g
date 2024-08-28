/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.net.URISyntaxException;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 *
 */

public class TopologyResourceIntegrationTest extends DataServiceBaseTestCase {

    private TopologyResource topologyResource;

    private MultivaluedMap<String, String> map;

    @Override
    public void onSetUp() {
        map = new MultivaluedMapImpl();
        topologyResource = new TopologyResource();
        attachDependencies(topologyResource);
    }

    @Test
    public void testGetDataThrowsUnsupportedOperationException() {
        try {
            topologyResource.getData("CANCEL_REQUEST_NOT_SUPPORTED", map);
            fail("UnsupportedOperationException should have been thrown");
        } catch (final UnsupportedOperationException e) {

        }
    }

    @Test
    public void testCheckParametersThrowsUnsupportedOperationException() {
        try {
            topologyResource.checkParameters(map);
            fail("UnsupportedOperationException should have been thrown");
        } catch (final UnsupportedOperationException e) {

        }
    }

    @Test
    public void testListConnectedCellsWithCorrectParameter() throws URISyntaxException {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(CELL_PARAM, "CELL68513");
        map.putSingle(NODE_PARAM, "7951,RNC01,Ericsson,0");
        DummyUriInfoImpl.setUriInfo(map, topologyResource);
        final String result = topologyResource.getListOfConnectedCells();
        assertJSONSucceeds(result);
    }

    @Test
    public void testListConnectedCellsWithMissingNodeParameter() throws URISyntaxException {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        DummyUriInfoImpl.setUriInfo(map, topologyResource);
        final String result = topologyResource.getListOfConnectedCells();
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
        assertResultContains(result, NODE_PARAM);
    }

    @Test
    public void testListConnectedCellsWithMissingTypeParameter() throws URISyntaxException {
        map.clear();
        map.putSingle(NODE_PARAM, "7951");
        DummyUriInfoImpl.setUriInfo(map, topologyResource);
        final String result = topologyResource.getListOfConnectedCells();
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
        assertResultContains(result, TYPE_PARAM);
    }

    @Test
    public void testListConnectedCellsWithIncorrectlyFormattedCellParameter() throws URISyntaxException {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        final String incompleteNodeValue = "7951";
        map.putSingle(NODE_PARAM, incompleteNodeValue);
        DummyUriInfoImpl.setUriInfo(map, topologyResource);
        final String result = topologyResource.getListOfConnectedCells();
        assertJSONErrorResult(result);
        assertResultContains(result, "Please input a valid value");
    }
}
