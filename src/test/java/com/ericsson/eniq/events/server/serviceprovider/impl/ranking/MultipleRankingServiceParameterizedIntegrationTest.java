package com.ericsson.eniq.events.server.serviceprovider.impl.ranking;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.RankingAnalysisTestDataProvider;

/**
 * @author ejedmar
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:com/ericsson/eniq/events/server/serviceprovider/2G3G4G-service-context.xml" })
public class MultipleRankingServiceParameterizedIntegrationTest extends ServiceBaseTest {

    @Resource(name = "multipleRankingService")
    private MultipleRankingService multipleRankingService;

    @Test
    @Parameters(source = RankingAnalysisTestDataProvider.class)
    public void test(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, multipleRankingService);
    }
}
