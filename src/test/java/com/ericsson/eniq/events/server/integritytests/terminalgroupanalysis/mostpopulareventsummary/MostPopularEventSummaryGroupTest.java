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
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR_EVENT_SUMMARY;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.SECOND_MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.THIRD_MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForMostPopularTacInLteTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForMostPopularTacInSgehTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForSecondMostPopularTac;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForThirdPopularTacInLteTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noSuccessesForMostPopularTac;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noSuccessesForSecondMostPopularTac;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BASE_URI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostPopularEventSummaryGroupTest extends
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
        final TablesPopulatorForOneWeekQuery populator = new TablesPopulatorForOneWeekQuery(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
    }

    @Test
    public void testGetGroupMostPopularEventSummary_OneWeek() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS
                + "/" + MOST_POPULAR_EVENT_SUMMARY);
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularEventSummaryGroupResult> results = getTranslator().translateResult(result,
                MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(4));
        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getTacGroupName(), is(MOST_POPULAR_TAC_GROUP));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgehTable + noErrorsForMostPopularTacInLteTable;
        assertThat(mostPopularTacGroup.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTac * 2;
        assertThat(mostPopularTacGroup.getNoSuccess(), is(expectedNoSuccesses));
        final int occurrences = expectedNoErrors + expectedNoSuccesses;
        assertThat(mostPopularTacGroup.getOccurrences(), is(occurrences));
        assertThat(mostPopularTacGroup.getSuccessRatio(), is(mostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(noErrorsForSecondMostPopularTac));
        assertThat(secondMostPopularTacGroup.getNoSuccess(), is(noSuccessesForSecondMostPopularTac));
        assertThat(secondMostPopularTacGroup.getOccurrences(), is(noErrorsForSecondMostPopularTac
                + noSuccessesForSecondMostPopularTac));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(),
                is(secondMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(), is(noErrorsForThirdPopularTacInLteTable));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(thirdMostPopularTacGroup.getOccurrences(), is(noErrorsForThirdPopularTacInLteTable));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(),
                is(thirdMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult fourthMostPopularTacGroup = results.get(3);
        assertThat(fourthMostPopularTacGroup.getTacGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(fourthMostPopularTacGroup.getNoErrors(), is(2));
        assertThat(fourthMostPopularTacGroup.getNoSuccess(), is(1));
        assertThat(fourthMostPopularTacGroup.getOccurrences(), is(3));
        assertThat(fourthMostPopularTacGroup.getSuccessRatio(),
                is(fourthMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(fourthMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

    }

}
