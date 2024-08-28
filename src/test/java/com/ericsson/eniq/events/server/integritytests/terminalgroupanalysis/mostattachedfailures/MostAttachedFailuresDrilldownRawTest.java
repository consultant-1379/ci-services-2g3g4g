/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostattachedfailures;

import java.util.List;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostAttachedFailuresDrilldownResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_ATTACHED_FAILURES;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FORTY_EIGHT_HOURS;
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
public class MostAttachedFailuresDrilldownRawTest extends
        TestsWithTemporaryTablesBaseTestCase<TerminalMostAttachedFailuresDrilldownResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    private MostAttachedFailuresTablesPopulator populator;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        populator = new MostAttachedFailuresTablesPopulator(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryGroupTable();
        populator.populateTemporaryTablesForLast3Minutes();
        populator.populateTemporaryTablesForLast48Hours();
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_48Hours() throws Exception {
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(EXCLUSIVE_TAC_GROUP,
                FORTY_EIGHT_HOURS);
        validateResultForExclusiveTacGroup(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_FiveMinutes() throws Exception {
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(
                MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_FiveMinutes() throws Exception {
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(EXCLUSIVE_TAC_GROUP,
                FIVE_MINUTES);
        validateResultForExclusiveTacGroup(translatedResults);
    }

    private void validateResultForExclusiveTacGroup(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getNoErrors(), is(1));
    }

    private List<TerminalMostAttachedFailuresDrilldownResult> runQuery(final String tacGroup, final String time)
            throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = getTranslator().translateResult(
                result, TerminalMostAttachedFailuresDrilldownResult.class);
        return translatedResults;
    }

    private void validateResultForRegularTacGroup(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.WORST_TAC));
        assertThat(resultsForTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_WORST_TAC));
        assertThat(resultsForTac.getMarketingName(),
                is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_WORST_TAC));
        assertThat(resultsForTac.getNoErrors(), is(2));
    }

}
