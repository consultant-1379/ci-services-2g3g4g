package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.resources.EventVolumeResourceConstants.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicy;
import com.ericsson.eniq.events.server.query.TimeRangeSelector;
import com.ericsson.eniq.events.server.services.DataService;
import com.ericsson.eniq.events.server.services.exclusivetacs.ExclusiveTACHandler;
import com.ericsson.eniq.events.server.templates.exception.ResourceInitializationException;
import com.ericsson.eniq.events.server.templates.mappingengine.TemplateMappingEngine;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.LoadBalancingPolicyService;
import com.ericsson.eniq.events.server.utils.QueryUtils;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.techpacks.RawTableFetcher;
import com.ericsson.eniq.events.server.utils.techpacks.TechPackListFactory;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * User: lmimmfn
 * Date: 25-May-2011
 * Time: 10:43:39
 * Class Responsibilities:
 */
public class EventVolumeResourceMockedTest extends BaseJMockUnitTest {

    private EventVolumeResource objToTest;

    ApplicationConfigManager mockApplicationConfigManager;

    DataService mockDataService;

    QueryUtils queryUtils;

    LoadBalancingPolicyService mockLoadBalancingPolicyService;

    TechPackListFactory mockedTechPackTableFactory;

    private TestUtilsMock templateUtils;

    RawTableFetcher mockedRawTableFetcher;

    DataService dataService;

    TemplateMappingEngine mockTemplateMappingEngine;

    ExclusiveTACHandler mockExclusiveTACHandler;

    TimeRangeSelector mockTimeRangeSelector;

    @Before
    public void setUp() throws Exception {

        objToTest = new EventVolumeResource();
        queryUtils = new QueryUtils();

        mockLoadBalancingPolicyService = mockery.mock(LoadBalancingPolicyService.class);
        mockedTechPackTableFactory = mockery.mock(TechPackListFactory.class);

        mockTemplateMappingEngine = mockery.mock(TemplateMappingEngine.class);
        objToTest.setTemplateMappingEngine(mockTemplateMappingEngine);

        mockedRawTableFetcher = mockery.mock(RawTableFetcher.class);
        objToTest.setRawTableFetcher(mockedRawTableFetcher);
        objToTest.setTechPackListFactory(mockedTechPackTableFactory);
        objToTest.setQueryUtils(queryUtils);

        mockApplicationConfigManager = mockery.mock(ApplicationConfigManager.class);
        queryUtils.setApplicationConfigManager(mockApplicationConfigManager);
        objToTest.setApplicationConfigManager(mockApplicationConfigManager);
        mockery.checking(new Expectations() {
            {
                allowing(mockApplicationConfigManager).isDataTieringEnabled();
            }
        });
        objToTest.setLoadBalancingPolicyService(mockLoadBalancingPolicyService);

        templateUtils = new TestUtilsMock();
        templateUtils.applicationStartup();
        objToTest.setTemplateUtils(templateUtils);

        dataService = mockery.mock(DataService.class);
        objToTest.setDataService(dataService);
        mockExclusiveTACHandler = mockery.mock(ExclusiveTACHandler.class);
        objToTest.setExclusiveTACHandler(mockExclusiveTACHandler);
        mockery.checking(new Expectations() {
            {
                allowing(mockExclusiveTACHandler).queryIsExclusiveTacRelated(with(any(String.class)),
                        with(any(String.class)));
            }
        });
        mockTimeRangeSelector = mockery.mock(TimeRangeSelector.class);
        objToTest.setTimeRangeSelector(mockTimeRangeSelector);
        mockery.checking(new Expectations() {
            {
                allowing(mockTimeRangeSelector).getTimeRangeType(with(any(String.class)), with(any(boolean.class)),
                        with(any(boolean.class)));
            }

        });
    }

    private void expectGetOnLoadBalancingPolicyService() {

        mockery.checking(new Expectations() {
            {
                one(mockLoadBalancingPolicyService).getLoadBalancingPolicy(with(any(MultivaluedMap.class)));
            }
        });

    }

    private void setupApplicationConfigManagerMock(final ApplicationConfigManager appConfMgrMock) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_SGEH);
        techPacks.add(EVENT_E_LTE);
        mockery.checking(new Expectations() {
            {
                atLeast(0).of(appConfMgrMock).getOneMinuteAggregation();
                will(returnValue(true));
                atLeast(1).of(appConfMgrMock).getTimeDelayOneMinuteData(techPacks);
                will(returnValue(0));
                atLeast(1).of(appConfMgrMock).getTimeDelayFifteenMinuteData(techPacks);
                will(returnValue(0));
                atLeast(1).of(appConfMgrMock).getTimeDelayDayData(techPacks);
                will(returnValue(0));
            }
        });
    }

    @Test
    public void testDayInterval() throws Exception {
        final String requestId = "1234";
        final String tzOffset = "+0100";
        final int expectedInterval = 1440;
        final MultivaluedMap<String, String> requestParameters = createRequestParameters(tzOffset, "10052011",
                "24052011", "0000", "0100");

        (templateUtils).setExpectedInterval(expectedInterval);

        setupApplicationConfigManagerMock(mockApplicationConfigManager);
        expectGetOnLoadBalancingPolicyService();
        expectOnTechPackTableFactory(EventDataSourceType.AGGREGATED_DAY.toString());
        expectGetTemplate(requestParameters);
        expectCallOnDataService(requestId, tzOffset);
        objToTest.getData(requestId, requestParameters);
        (templateUtils).verify();
        mockery.assertIsSatisfied();
    }

    private void expectCallOnDataService(final String requestId, final String tzOffset) {
        mockery.checking(new Expectations() {
            {
                one(dataService).getSamplingChartData(with(same(requestId)), with(any(String.class)),
                        with(any(Map.class)), with(any(String[].class)), with(any(String.class)),
                        with(any(String.class)), with(any(String.class)), with(same(tzOffset)),
                        with(any(LoadBalancingPolicy.class)));
            }
        });

    }

    private void expectGetTemplate(final MultivaluedMap<String, String> requestParameters) {
        mockery.checking(new Expectations() {
            {
                one(mockTemplateMappingEngine).getTemplate(EVENT_VOLUME, requestParameters, null, "");
                will(returnValue("network/q_network_analysis_summary_sample.vm"));
            }
        });

    }

    @Test
    public void test15MinuteInterval() throws Exception {
        final String requestId = "1234";
        final String tzOffset = "+0100";
        final int expectedInterval = 15;
        final MultivaluedMap<String, String> requestParameters = createRequestParameters(tzOffset, "20052011",
                "24052011", "0000", "0000");

        (templateUtils).setExpectedInterval(expectedInterval);
        setupApplicationConfigManagerMock(mockApplicationConfigManager);
        expectGetOnLoadBalancingPolicyService();
        expectOnTechPackTableFactory(EventDataSourceType.AGGREGATED_15MIN.toString());
        expectGetTemplate(requestParameters);
        expectCallOnDataService(requestId, tzOffset);
        objToTest.getData(requestId, requestParameters);
        (templateUtils).verify();
        mockery.assertIsSatisfied();
    }

    private void expectOnTechPackTableFactory(final String timerange) {

        final List<String> rawErrSgehTables = new ArrayList<String>();
        rawErrSgehTables.add(EVENT_E_SGEH_ERR_RAW);
        final List<String> rawSucSgehTables = new ArrayList<String>();
        rawSucSgehTables.add(EVENT_E_SGEH_SUC_RAW);
        mockery.checking(new Expectations() {
            {
                allowing(mockedTechPackTableFactory).shouldQueryUseAggregationView(with(equal(NO_TYPE)),
                        with(equal(timerange)), with(any(Map.class)));

                allowing(mockedTechPackTableFactory).getMatchingDIMTechPack(EVENT_E_SGEH);
                allowing(mockedTechPackTableFactory).getMatchingDIMTechPack(EVENT_E_LTE);
                allowing(mockedTechPackTableFactory).getErrorAggregationView(with(any(String.class)),
                        with(any(String.class)), with(any(String.class)), with(any(Map.class)));
                allowing(mockedTechPackTableFactory).getSuccessAggregationView(with(any(String.class)),
                        with(any(String.class)), with(any(String.class)), with(any(Map.class)));
                one(mockedRawTableFetcher).getRawErrTables(with(any(FormattedDateTimeRange.class)),
                        with(equal(EVENT_E_SGEH)));
                will(returnValue(rawErrSgehTables));
                one(mockedRawTableFetcher).getRawSucTables(with(any(FormattedDateTimeRange.class)),
                        with(equal(EVENT_E_SGEH)));
                will(returnValue(rawSucSgehTables));
                one(mockedRawTableFetcher).getRawErrTables(with(any(FormattedDateTimeRange.class)),
                        with(equal(EVENT_E_LTE)));
                one(mockedRawTableFetcher).getRawSucTables(with(any(FormattedDateTimeRange.class)),
                        with(equal(EVENT_E_LTE)));

            }
        });

    }

    private MultivaluedMap<String, String> createRequestParameters(final String tzOffset, final String dateFrom,
            final String dateTo, final String timeFrom, final String timeTo) {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(ApplicationConstants.DISPLAY_PARAM, ApplicationConstants.CHART_PARAM);
        requestParameters.add(ApplicationConstants.DISPLAY_PARAM, ApplicationConstants.CHART_PARAM);
        requestParameters.add(ApplicationConstants.DATE_FROM_QUERY_PARAM, dateFrom);
        requestParameters.add(ApplicationConstants.DATE_TO_QUERY_PARAM, dateTo);
        requestParameters.add(ApplicationConstants.TIME_FROM_QUERY_PARAM, timeFrom);
        requestParameters.add(ApplicationConstants.TIME_TO_QUERY_PARAM, timeTo);
        requestParameters.add(ApplicationConstants.TZ_OFFSET, tzOffset);
        requestParameters.add(ApplicationConstants.MAX_ROWS, "5000");
        return requestParameters;
    }

    @Test
    public void test1MinuteInterval() throws Exception {
        final String requestId = "1234";
        final String tzOffset = "+0100";
        final MultivaluedMap<String, String> requestParameters = createRequestParameters(tzOffset, "10052011",
                "10052011", "0000", "0200");

        setupApplicationConfigManagerMock(mockApplicationConfigManager);
        expectGetOnLoadBalancingPolicyService();
        expectOnTechPackTableFactory(EventDataSourceType.AGGREGATED_1MIN.toString());
        expectGetTemplate(requestParameters);
        expectCallOnDataService(requestId, tzOffset);
        objToTest.getData(requestId, requestParameters);
        mockery.assertIsSatisfied();

    }

    /**
     * Mock object to setup expected Interval parameter
     */
    public class TestUtilsMock extends TemplateUtils {

        private int expectedInterval;

        private int actualInterval;

        public void setExpectedInterval(final int interval) {
            expectedInterval = interval;
        }

        @Override
        public String getQueryFromTemplate(final String templateFile, final Map<String, ?> parameters)
                throws ResourceInitializationException {
            actualInterval = (Integer) parameters.get(INTERVAL_PARAM);
            return super.getQueryFromTemplate(templateFile, parameters);
        }

        public void verify() {
            assertEquals(expectedInterval, actualInterval);
        }
    }

}
