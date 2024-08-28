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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.NODE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_SGSN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EVENT_SOURCE_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.QCI_ERR_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.QCI_SUC_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_MME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_REQUEST_ID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class QOSWithOnlySuccessEventsTest extends TestsWithTemporaryTablesBaseTestCase<QOSStatisticsSummaryResult> {

    private QOSStatisticsResource qosStatisticsResource;

    private final int noSuccessesForAPN = 2;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        qosStatisticsResource = new QOSStatisticsResource();
        attachDependencies(qosStatisticsResource);
        createAggregationTables();
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, getColumnsForRawTables());
        insertData();
    }

    @Test
    public void testImpactedSubscriberCalculationForQOSSummaryView() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(NODE_PARAM, SAMPLE_MME);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "10");
        final String json = qosStatisticsResource.getData(SAMPLE_REQUEST_ID, map);
        System.out.println(json);
        jsonAssertUtils.assertJSONSucceeds(json);
        final List<QOSStatisticsSummaryResult> qciSummaryResult = getTranslator().translateResult(json,
                QOSStatisticsSummaryResult.class);
        validateResult(qciSummaryResult);
    }

    private void validateResult(final List<QOSStatisticsSummaryResult> results) {
        assertThat(results.size(), is(10));
        final QOSStatisticsSummaryResult resultForQCI1 = results.get(0);
        assertThat(resultForQCI1.getNoErrors(), is(0));
        assertThat(resultForQCI1.getNoSuccesses(), is(noSuccessesForAPN));
        assertThat(resultForQCI1.getImpactedSubscribers(), is(0));
        verifyRemainingValuesAreAll0(results);
    }

    private void verifyRemainingValuesAreAll0(final List<QOSStatisticsSummaryResult> results) {
        for (int i = 1; i < results.size(); i++) {
            final QOSStatisticsSummaryResult result = results.get(i);
            assertThat(result.getNoErrors(), is(0));
            assertThat(result.getNoSuccesses(), is(0));
            assertThat(result.getImpactedSubscribers(), is(0));
        }
    }

    private void insertData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, 0, 0, timestamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, 0, noSuccessesForAPN, timestamp);
    }

    private void insertRowIntoAggTable(final String table, final int noErrors, final int noSuccesses,
            final String timestamp) throws SQLException {
        final Map<String, Object> valuesForAggErrTable = new HashMap<String, Object>();
        valuesForAggErrTable.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        valuesForAggErrTable.putAll(QCIColumns.getDefaultQCIValues());
        valuesForAggErrTable.put(QCI_ERR_1, noErrors);
        valuesForAggErrTable.put(QCI_SUC_1, noSuccesses);
        valuesForAggErrTable.put(DATETIME_ID, timestamp);
        insertRow(table, valuesForAggErrTable);
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columns = getColumnsForAggregationTables();
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, columns);
    }

    private Collection<String> getColumnsForRawTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(EVENT_SOURCE_NAME);
        columns.add(IMSI);
        columns.add(TAC);
        columns.add(DATETIME_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }

    private Collection<String> getColumnsForAggregationTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(EVENT_SOURCE_NAME);
        columns.add(DATETIME_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }

}
