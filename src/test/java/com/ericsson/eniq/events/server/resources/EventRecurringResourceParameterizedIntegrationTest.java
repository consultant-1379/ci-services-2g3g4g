/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.eniq.events.server.resources.automation.ResourceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.EventRecurringTestDataProvider;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;

/**
 * @author ejedmar
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
public class EventRecurringResourceParameterizedIntegrationTest extends ResourceBaseTest {

    @Resource(name = "eventRecurringResource")
    private EventRecurringResource eventRecurringResource;

    @Test
    @Parameters(source = EventRecurringTestDataProvider.class)
    public void testGetRecurErrorSummaryData(final MultivaluedMap<String, String> requestParameters) throws Exception {
        DummyUriInfoImpl.setUriInfo(requestParameters, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
