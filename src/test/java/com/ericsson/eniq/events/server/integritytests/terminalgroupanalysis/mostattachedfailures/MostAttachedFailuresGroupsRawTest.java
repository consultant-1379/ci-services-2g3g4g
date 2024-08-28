/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostattachedfailures;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostAttachedFailuresGroupResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MOST_ATTACHED_FAILURES;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TERMINAL_GROUP_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BASE_URI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostAttachedFailuresGroupsRawTest extends
        TestsWithTemporaryTablesBaseTestCase<MostAttachedFailuresGroupResult> {

    private static final int IMSI1 = 123456789;

    private static final String WORST_TAC_GROUP = "worstTacGroup";

    private static final int WORST_TAC = 123456;

    private static final int SECOND_WORST_TAC = 9999;

    private static final String SECOND_WORST_TAC_GROUP = "secondWorstTacGroup";

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
        createTemporaryTables();
        populateTemporaryTables();
    }

    @Test
    public void testGetGroupMostAttachedFailuresData_FiveMinutes() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresGroupResult> results = getTranslator().translateResult(result,
                MostAttachedFailuresGroupResult.class);
        validateResults(results);

    }

    private void validateResults(final List<MostAttachedFailuresGroupResult> results) {
        assertThat(results.size(), is(3));
        final MostAttachedFailuresGroupResult worstGroup = results.get(0);
        assertThat(worstGroup.getGroupName(), is(WORST_TAC_GROUP));
        assertThat(worstGroup.getNoErrors(), is(3));
        assertThat(worstGroup.getNoTotalErrSubscribers(), is(1));

        final MostAttachedFailuresGroupResult secondWorstGroup = results.get(1);
        assertThat(secondWorstGroup.getGroupName(), is(SECOND_WORST_TAC_GROUP));
        assertThat(secondWorstGroup.getNoErrors(), is(2));
        assertThat(secondWorstGroup.getNoTotalErrSubscribers(), is(1));

        final MostAttachedFailuresGroupResult thirdWorstGroup = results.get(2);
        assertThat(thirdWorstGroup.getGroupName(), is(EXCLUSIVE_TAC_GROUP));
        assertThat(thirdWorstGroup.getNoErrors(), is(1));
        assertThat(thirdWorstGroup.getNoTotalErrSubscribers(), is(1));
    }

    private void populateTemporaryTables() throws SQLException {

        final String timeStamp = DateTimeUtilities.getDateTimeMinus3Minutes();
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, WORST_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, WORST_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, WORST_TAC, timeStamp);
        populateTACGroupTable(WORST_TAC, WORST_TAC_GROUP);
        populateTACGroupTable(SAMPLE_EXCLUSIVE_TAC, WORST_TAC_GROUP);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SECOND_WORST_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SECOND_WORST_TAC, timeStamp);
        populateTACGroupTable(SECOND_WORST_TAC, SECOND_WORST_TAC_GROUP);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
    }

    private void insertRowIntoRawTable(final String table, final int tac, final String timeStamp) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(EVENT_ID, 0);
        valuesForRawTable.put(TAC, tac);
        valuesForRawTable.put(IMSI, IMSI1);
        valuesForRawTable.put(DATETIME_ID, timeStamp);
        insertRow(table, valuesForRawTable);
    }

    private void populateTACGroupTable(final int tac, final String tacGroup) throws SQLException {
        final Map<String, Object> valuesForTacGroupTable = new HashMap<String, Object>();
        valuesForTacGroupTable.put(GROUP_NAME, tacGroup);
        valuesForTacGroupTable.put(TAC, tac);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTacGroupTable);
    }

    private void createTemporaryTables() throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForRawTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForRawTable);
    }
}
