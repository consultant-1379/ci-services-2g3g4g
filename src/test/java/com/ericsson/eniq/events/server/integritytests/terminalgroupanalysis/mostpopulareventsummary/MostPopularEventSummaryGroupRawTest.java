/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary;

import java.util.List;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularEventSummaryGroupResult;
import com.ericsson.eniq.events.server.test.queryresults.SuccessRatioCalculator;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR_EVENT_SUMMARY;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BASE_URI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostPopularEventSummaryGroupRawTest extends
        TestsWithTemporaryTablesBaseTestCase<MostPopularEventSummaryGroupResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
    }

    @Test
    public void testGetGroupMostPopularEventSummary_FiveMinutes() throws Exception {
        populateTablesAndData(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = getData(FIVE_MINUTES);
        validateResult(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryWithDataTieringOn30Min() throws Exception {
        populateTablesAndData(DateTimeUtilities.getDateTimeMinus25Minutes());
        jndiProperties.setUpDataTieringJNDIProperty();

        final String result = getData(THIRTY_MINUTES);
        final List<MostPopularEventSummaryGroupResult> results = getTranslator().translateResult(result,
                MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(4));
        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results.get(0);

        assertThat(mostPopularTacGroup.getTacGroupName(),
                is(RawTablesPopulatorForTerminalGroupAnalysis.MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoErrors(), is(2));
        assertThat(mostPopularTacGroup.getNoSuccess(), is(2));
        assertThat(mostPopularTacGroup.getOccurrences(), is(4));
        assertThat(mostPopularTacGroup.getSuccessRatio(), is(SuccessRatioCalculator.calculateSuccessRatio(4, 2)));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(2));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(),
                is(RawTablesPopulatorForTerminalGroupAnalysis.SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(0));
        assertThat(secondMostPopularTacGroup.getNoSuccess(), is(2));
        assertThat(secondMostPopularTacGroup.getOccurrences(), is(2));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(), is(SuccessRatioCalculator.calculateSuccessRatio(2, 2)));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularEventSummaryGroupResult fourthMostPopularTacGroup = results.get(2);
        assertThat(fourthMostPopularTacGroup.getTacGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(fourthMostPopularTacGroup.getNoErrors(), is(1));
        assertThat(fourthMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(fourthMostPopularTacGroup.getOccurrences(), is(1));
        assertThat(fourthMostPopularTacGroup.getSuccessRatio(), is(SuccessRatioCalculator.calculateSuccessRatio(1, 0)));
        assertThat(fourthMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results.get(3);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(),
                is(RawTablesPopulatorForTerminalGroupAnalysis.THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(), is(1));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(thirdMostPopularTacGroup.getOccurrences(), is(1));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(), is(SuccessRatioCalculator.calculateSuccessRatio(1, 0)));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularEventSummaryGroupResult> results = getTranslator().translateResult(result,
                MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(4));
        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results.get(0);

        assertThat(mostPopularTacGroup.getTacGroupName(),
                is(RawTablesPopulatorForTerminalGroupAnalysis.MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoErrors(), is(2));
        assertThat(mostPopularTacGroup.getNoSuccess(), is(2));
        assertThat(mostPopularTacGroup.getOccurrences(), is(4));
        assertThat(mostPopularTacGroup.getSuccessRatio(), is(mostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(2));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(),
                is(RawTablesPopulatorForTerminalGroupAnalysis.SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(0));
        assertThat(secondMostPopularTacGroup.getNoSuccess(), is(3));
        assertThat(secondMostPopularTacGroup.getOccurrences(), is(3));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(),
                is(secondMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(),
                is(RawTablesPopulatorForTerminalGroupAnalysis.THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(), is(1));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(1));
        assertThat(thirdMostPopularTacGroup.getOccurrences(), is(2));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(),
                is(thirdMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult fourthMostPopularTacGroup = results.get(3);
        assertThat(fourthMostPopularTacGroup.getTacGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(fourthMostPopularTacGroup.getNoErrors(), is(1));
        assertThat(fourthMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(fourthMostPopularTacGroup.getOccurrences(), is(1));
        assertThat(fourthMostPopularTacGroup.getSuccessRatio(),
                is(fourthMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(fourthMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

    }

    private String getData(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY;

        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        System.out.println(result);
        return result;
    }

    private void populateTablesAndData(final String time) throws Exception {
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(
                connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(time);
    }

}
