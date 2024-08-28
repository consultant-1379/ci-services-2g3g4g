/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.EventAnalysisTestDataProvider;

/**
 * @author ejedmar
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:com/ericsson/eniq/events/server/serviceprovider/2G3G4G-service-context.xml" })
public class EventAnalysisServiceParameterizedIntegrationTest extends ServiceBaseTest {

    @Resource(name = "eventAnalysisService")
    private EventAnalysisService eventAnalysisService;

    @Test
    @Parameters(source = EventAnalysisTestDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, eventAnalysisService);
    }
}