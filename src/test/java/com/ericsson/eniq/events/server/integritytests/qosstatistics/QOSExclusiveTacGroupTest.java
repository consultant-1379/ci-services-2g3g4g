/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.QOSStatisticsResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.QOSStatisticsSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EXCLUSIVE_TAC_GROUP_NAME;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.QCI_ERR_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.QCI_SUC_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_REQUEST_ID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class QOSExclusiveTacGroupTest extends TestsWithTemporaryTablesBaseTestCase<QOSStatisticsSummaryResult> {

    private QOSStatisticsResource qosStatisticsResource;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        qosStatisticsResource = new QOSStatisticsResource();
        attachDependencies(qosStatisticsResource);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, getColumnsForRawTables());
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, getColumnsForRawTables());
    }

    @Test
    public void testResultsAreEmptyForTacGroupContainingJustExclusiveTac() throws Exception {
        insertData(DateTimeUtilities.getDateTimeMinus2Minutes());
        final List<QOSStatisticsSummaryResult> qciSummaryResult = runQuery(SAMPLE_TAC_GROUP, FIVE_MINUTES);
        validateResultsAllContainZeroes(qciSummaryResult);
    }

    private void validateResultsAllContainZeroes(final List<QOSStatisticsSummaryResult> results) {
        assertThat(results.size(), is(10));
        for (final QOSStatisticsSummaryResult result : results) {
            assertThat(result.getNoErrors(), is(0));
            assertThat(result.getNoSuccesses(), is(0));
            assertThat(result.getImpactedSubscribers(), is(0));
        }
    }

    @Test
    public void testExclusiveTacGroupResultsIncludedWhenQueryForExclusiveTacGroup() throws Exception {
        insertData(DateTimeUtilities.getDateTimeMinus2Minutes());
        final List<QOSStatisticsSummaryResult> qciSummaryResult = runQuery(EXCLUSIVE_TAC_GROUP_NAME, FIVE_MINUTES);
        validateResultForExclusiveTacGroup(qciSummaryResult);
    }

    @Test
    public void testExclusiveTacGroupResultsIncludedWhenQueryForExclusiveTacGroupWithDataTiering() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        insertData(DateTimeUtilities.getDateTimeMinus25Minutes());
        final List<QOSStatisticsSummaryResult> qciSummaryResult = runQuery(EXCLUSIVE_TAC_GROUP_NAME, THIRTY_MINUTES);
        validateResultForExclusiveTacGroup(qciSummaryResult);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private List<QOSStatisticsSummaryResult> runQuery(final String groupName, final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, groupName);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "10");
        final String json = qosStatisticsResource.getData(SAMPLE_REQUEST_ID, map);
        System.out.println(json);
        jsonAssertUtils.assertJSONSucceeds(json);
        final List<QOSStatisticsSummaryResult> qciSummaryResult = getTranslator().translateResult(json,
                QOSStatisticsSummaryResult.class);
        return qciSummaryResult;
    }

    private void validateResultForExclusiveTacGroup(final List<QOSStatisticsSummaryResult> results) {
        assertThat(results.size(), is(10));

        final QOSStatisticsSummaryResult resultForQCI1 = results.get(0);
        assertThat(resultForQCI1.getNoErrors(), is(4));
        assertThat(resultForQCI1.getNoSuccesses(), is(2));
        assertThat(resultForQCI1.getImpactedSubscribers(), is(1));
    }

    private void insertData(final String timestamp) throws SQLException {
        insertRowIntoRawTableWithValuesForQCI1(TEMP_EVENT_E_LTE_ERR_RAW, timestamp);
        insertRowIntoRawTableWithValuesForQCI1(TEMP_EVENT_E_LTE_ERR_RAW, timestamp);
        insertRowIntoRawTableWithValuesForQCI1(TEMP_EVENT_E_LTE_ERR_RAW, timestamp);
        insertRowIntoRawTableWithValuesForQCI1(TEMP_EVENT_E_LTE_ERR_RAW, timestamp);
        insertRowIntoRawTableWithValuesForQCI1(TEMP_EVENT_E_LTE_SUC_RAW, timestamp);
        insertRowIntoRawTableWithValuesForQCI1(TEMP_EVENT_E_LTE_SUC_RAW, timestamp);
    }

    private void insertRowIntoRawTableWithValuesForQCI1(final String table, final String timestamp) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        valuesForRawTable.put(IMSI, SAMPLE_IMSI);
        valuesForRawTable.put(DATETIME_ID, timestamp);
        valuesForRawTable.putAll(QCIColumns.getDefaultQCIValues());
        valuesForRawTable.put(QCI_ERR_1, 1);
        valuesForRawTable.put(QCI_SUC_1, 1);
        insertRow(table, valuesForRawTable);
    }

    private Collection<String> getColumnsForRawTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(TAC);
        columns.add(IMSI);
        columns.add(DATETIME_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }
}
