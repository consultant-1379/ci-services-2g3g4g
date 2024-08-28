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
import com.ericsson.eniq.events.server.test.queryresults.MostPopularTerminalsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_POPULAR;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MANUFACTURER_FOR_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MARKETING_NAME_FOR_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.SECOND_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.THIRD_MOST_POPULAR_TAC;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForMostPopularTacInLteTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForMostPopularTacInSgehTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForSecondMostPopularTac;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noErrorsForThirdPopularTacInLteTable;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noSuccessesForMostPopularTac;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.noSuccessesForSecondMostPopularTac;
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
public class MostPopularTerminalsTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularTerminalsResult> {

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
    public void testGetTerminalMostPopularEvents_OneWeek() throws Exception {
        final TablesPopulatorForOneWeekQuery populator = new TablesPopulatorForOneWeekQuery(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_ANALYSIS + "/"
                + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularTerminalsResult> results = getTranslator().translateResult(result,
                MostPopularTerminalsResult.class);
        assertThat(results.size(), is(3));
        final MostPopularTerminalsResult mostPopularTac = results.get(0);
        assertThat(mostPopularTac.getTac(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getNoEvents(), is(noErrorsForMostPopularTacInSgehTable
                + noErrorsForMostPopularTacInLteTable + noSuccessesForMostPopularTac * 2));

        final MostPopularTerminalsResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTac(), is(SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(noErrorsForSecondMostPopularTac
                + noSuccessesForSecondMostPopularTac));

        final MostPopularTerminalsResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTac(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(thirdMostPopularTacGroup.getManufacturer(), is(""));
        assertThat(thirdMostPopularTacGroup.getMarketingName(), is(""));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(noErrorsForThirdPopularTacInLteTable));

    }

}
