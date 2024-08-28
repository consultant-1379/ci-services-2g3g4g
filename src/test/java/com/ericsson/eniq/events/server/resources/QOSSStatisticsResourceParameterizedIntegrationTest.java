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
import com.ericsson.eniq.events.server.resources.automation.dataproviders.QOSSStatisticsTestDataProvider;

/**
 * @author ejedmar
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
public class QOSSStatisticsResourceParameterizedIntegrationTest extends ResourceBaseTest {

    @Resource(name = "qosStatisticsResource")
    private QOSStatisticsResource qosStatisticsResource;

    @Test
    @Parameters(source = QOSSStatisticsTestDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(qosStatisticsResource.getData(TEST_REQUEST_ID, requestParameters));
    }

}
