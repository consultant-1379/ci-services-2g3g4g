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
import com.ericsson.eniq.events.server.resources.automation.dataproviders.SubsessionBITestDataProvider;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;

/**
 * @author ejedmar
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
public class SubsessionBIResourceParameterizedIntegrationTest extends ResourceBaseTest {

    @Resource(name = "subsessionBIResource")
    private SubsessionBIResource subsessionBIResource;

    @Test
    @Parameters(source = SubsessionBITestDataProvider.class)
    public void testGetSubBIAPNData(final MultivaluedMap<String, String> requestParameters) throws Exception {
        DummyUriInfoImpl.setUriInfo(requestParameters, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIAPNData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
