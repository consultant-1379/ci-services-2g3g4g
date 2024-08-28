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
import com.ericsson.eniq.events.server.test.queryresults.MostAttachedFailuresGroupResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_ATTACHED_FAILURES;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
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
public class MostAttachedFailuresGroupsTest extends
        TestsWithTemporaryTablesBaseTestCase<MostAttachedFailuresGroupResult> {

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
        populator.populateTemporaryTablesForLast48Hours();
    }

    @Test
    public void testGetGroupMostAttachedFailuresData_OneWeek() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostAttachedFailuresGroupResult> results = getTranslator().translateResult(result,
                MostAttachedFailuresGroupResult.class);
        assertThat(results.size(), is(3));
        final MostAttachedFailuresGroupResult worstTacGroup = results.get(0);
        assertThat(worstTacGroup.getGroupName(), is(MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP));
        assertThat(worstTacGroup.getNoErrors(), is(MostAttachedFailuresTablesPopulator.noErrorsForWorstTac * 2));
        assertThat(worstTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostAttachedFailuresGroupResult secondWorstTacGroup = results.get(1);
        assertThat(secondWorstTacGroup.getGroupName(), is(MostAttachedFailuresTablesPopulator.SECOND_WORST_TAC_GROUP));
        assertThat(secondWorstTacGroup.getNoErrors(), is(MostAttachedFailuresTablesPopulator.noErrorsForSecondWorstTac));
        assertThat(secondWorstTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostAttachedFailuresGroupResult thirdWorstTacGroup = results.get(2);
        assertThat(thirdWorstTacGroup.getGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(thirdWorstTacGroup.getNoErrors(), is(1));
        assertThat(thirdWorstTacGroup.getNoTotalErrSubscribers(), is(1));
    }
}
