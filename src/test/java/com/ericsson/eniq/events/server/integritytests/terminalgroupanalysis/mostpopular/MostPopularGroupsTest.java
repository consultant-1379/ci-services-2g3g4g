/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopular;

import java.util.List;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularGroupsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR;
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
public class MostPopularGroupsTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularGroupsResult> {

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
    public void testGetGroupMostPopularEvents_OneWeek() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS
                + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularGroupsResult> results = getTranslator().translateResult(result,
                MostPopularGroupsResult.class);
        assertThat(results.size(), is(4));
        final MostPopularGroupsResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getTacGroupName(), is(MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoEvents(), is(noErrorsForMostPopularTacInSgehTable
                + noErrorsForMostPopularTacInLteTable + noSuccessesForMostPopularTac * 2));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularGroupsResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(noErrorsForSecondMostPopularTac
                + noSuccessesForSecondMostPopularTac));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularGroupsResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(noErrorsForThirdPopularTacInLteTable));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularGroupsResult exclusiveTacGroup = results.get(3);
        assertThat(exclusiveTacGroup.getTacGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(exclusiveTacGroup.getNoEvents(), is(3));
        assertThat(exclusiveTacGroup.getNoTotalErrSubscribers(), is(1));

    }

}
