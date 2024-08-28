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
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostPopularDrilldownResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MANUFACTURER_FOR_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MARKETING_NAME_FOR_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForMostPopularTacInLteTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForMostPopularTacInSgehTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noSuccessesForMostPopularTac;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BASE_URI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostPopularDrilldownTest extends TestsWithTemporaryTablesBaseTestCase<TerminalMostPopularDrilldownResult> {

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
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_OneWeek() throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup(results);
    }

    private void validateResultForExclusiveTacGroup(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(3));

    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_OneWeek() throws Exception {
        final List<TerminalMostPopularDrilldownResult> result = runQuery(MOST_POPULAR_TAC_GROUP);
        validateResultForRegularTacGroup(result);
    }

    private List<TerminalMostPopularDrilldownResult> runQuery(final String tacGroup) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS
                + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularDrilldownResult.class);
        return results;
    }

    private void validateResultForRegularTacGroup(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(noErrorsForMostPopularTacInSgehTable
                + noSuccessesForMostPopularTac + noErrorsForMostPopularTacInLteTable + noSuccessesForMostPopularTac));

    }

}
