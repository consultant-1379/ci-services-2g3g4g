/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_REQUEST_ID;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.eniq.events.server.resources.automation.ResourceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.KPIResourceTestDataProvider;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;

/**
 * @author ejedmar
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
public class KPIResourceParameterizedIntegrationTest extends ResourceBaseTest {

    @Resource(name = "kpiResource")
    private KPIResource kpiResource;

    @Test
    @Parameters(source = KPIResourceTestDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) throws Exception {
        DummyUriInfoImpl.setUriInfo(requestParameters, kpiResource);
        jsonAssertUtils.assertJSONSucceeds(kpiResource.getData(TEST_REQUEST_ID, requestParameters));
    }

}
