/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary;

import java.net.URISyntaxException;
import java.util.List;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostPopularEventSummaryDrilldownResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR_EVENT_SUMMARY;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.MANUFACTURER_FOR_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.MARKETING_NAME_FOR_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.MOST_POPULAR_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BASE_URI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostPopularEventSummaryDrilldownRawTest extends
        TestsWithTemporaryTablesBaseTestCase<TerminalMostPopularEventSummaryDrilldownResult> {

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
    public void testGetGroupMostPopularEventSummaryDrilldown_FiveMinutes() throws Exception {
        final String result = runQuery(MOST_POPULAR_TAC_GROUP);
        validateResultForRegularTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_FiveMinutes() throws Exception {
        final String result = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup(result);
    }

    private void validateResultForExclusiveTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult resultForExclusiveTac = results.get(0);
        assertThat(resultForExclusiveTac.getTac(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getNoErrors(), is(1));
        assertThat(resultForExclusiveTac.getNoSuccess(), is(0));
        assertThat(resultForExclusiveTac.getOccurrences(), is(1));
        assertThat(resultForExclusiveTac.getSuccessRatio(), is(resultForExclusiveTac.calculateExpectedSuccessRatio()));

    }

    private String runQuery(final String tacGroup) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS
                + "/" + MOST_POPULAR_EVENT_SUMMARY);
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        System.out.println(result);
        return result;
    }

    private void validateResultForRegularTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult tacInMostPopularTacGroup = results.get(0);
        assertThat(tacInMostPopularTacGroup.getTac(), is(MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getNoErrors(), is(2));
        assertThat(tacInMostPopularTacGroup.getNoSuccess(), is(2));
        assertThat(tacInMostPopularTacGroup.getOccurrences(), is(4));
        assertThat(tacInMostPopularTacGroup.getSuccessRatio(),
                is(tacInMostPopularTacGroup.calculateExpectedSuccessRatio()));

    }

}
