/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopular;

import java.util.List;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularGroupsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.SECOND_MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.THIRD_MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BASE_URI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostPopularGroupsRawTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularGroupsResult> {

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
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(
                connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
    }

    @Test
    public void testGetGroupMostPopularEvents_FiveMinutes() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
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
        assertThat(mostPopularTacGroup.getNoEvents(), is(4));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(2));

        final MostPopularGroupsResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(3));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularGroupsResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(2));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularGroupsResult exclusiveTacGroup = results.get(3);
        assertThat(exclusiveTacGroup.getTacGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(exclusiveTacGroup.getNoEvents(), is(1));
        assertThat(exclusiveTacGroup.getNoTotalErrSubscribers(), is(1));

    }

}
