/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.eniq.events.server.resources.automation.ResourceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.CauseCodeTablesSCCTestDataProvider;

/**
 * @author ejedmar
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
public class CauseCodeTablesSCCResourceParameterizedIntegrationTest extends ResourceBaseTest {

    @Resource(name = "causeCodeTablesSCCResource")
    private CauseCodeTablesSCCResource causeCodeTablesSCCResource;

    @Test
    @Parameters(source = CauseCodeTablesSCCTestDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        jsonAssertUtils.assertJSONSucceeds(causeCodeTablesSCCResource.getData(TEST_REQUEST_ID, requestParameters));
    }

}
