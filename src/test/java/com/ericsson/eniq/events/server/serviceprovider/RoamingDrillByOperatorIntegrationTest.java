/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.RoamingDrillByOperatorDataProvider;
import com.ericsson.eniq.events.server.serviceprovider.impl.roaminganalysis.RoamingDrillByOperatorService;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

/**
 * @author ezhelao
 * @since 06/02/2012
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:com/ericsson/eniq/events/server/serviceprovider/2G3G4G-service-context.xml"})
public class RoamingDrillByOperatorIntegrationTest extends ServiceBaseTest {
    @Resource(name = "roamingDrillByOperatorService")
    private RoamingDrillByOperatorService roamingDrillByOperatorService;


    @Test
    @Parameters(source = RoamingDrillByOperatorDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, roamingDrillByOperatorService);

    }

}
