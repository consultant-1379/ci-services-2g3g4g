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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.QCI_ERR_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.QCI_SUC_1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_REQUEST_ID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class QOSGroupImpactedSubscribersTest extends TestsWithTemporaryTablesBaseTestCase<QOSStatisticsSummaryResult> {

    private QOSStatisticsResource qosStatisticsResource;

    private final int noErrorsForAPN = 6;

    private final int noSuccessesForAPN = 2;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        qosStatisticsResource = new QOSStatisticsResource();
        attachDependencies(qosStatisticsResource);
        insertDataIntoTacGroupTable();
        createAggregationTables();
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, getColumnsForRawTables());
        insertData();
    }

    @Test
    public void testImpactedSubscriberCalculationForQOSSummaryView() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_TAC_GROUP);
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
        assertThat(resultForQCI1.getNoErrors(), is(noErrorsForAPN));
        assertThat(resultForQCI1.getNoSuccesses(), is(noSuccessesForAPN));
        assertThat(resultForQCI1.getImpactedSubscribers(), is(3));
        final QOSStatisticsSummaryResult resultForQCI2 = results.get(1);
        assertThat(resultForQCI2.getNoErrors(), is(0));
        assertThat(resultForQCI2.getNoSuccesses(), is(0));
        assertThat(resultForQCI2.getImpactedSubscribers(), is(0));

    }

    private void insertData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, noErrorsForAPN, 0, timestamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, 0, noSuccessesForAPN, timestamp);
        insertRowIntoRawTable(1234567, timestamp);
        insertRowIntoRawTable(1234567, timestamp);
        insertRowIntoRawTable(1234567, timestamp);
        insertRowIntoRawTable(11111, timestamp);
        insertRowIntoRawTable(999999, DateTimeUtilities.getDateTimeMinus36Hours());
        insertRowIntoRawTable(999999, DateTimeUtilities.getDateTimeMinus48Hours());
    }

    private void insertRowIntoAggTable(final String table, final int noErrors, final int noSuccesses,
            final String timestamp) throws SQLException {
        final Map<String, Object> valuesForAggErrTable = new HashMap<String, Object>();
        valuesForAggErrTable.put(TAC, SAMPLE_TAC);
        valuesForAggErrTable.putAll(QCIColumns.getDefaultQCIValues());
        valuesForAggErrTable.put(QCI_ERR_1, noErrors);
        valuesForAggErrTable.put(QCI_SUC_1, noSuccesses);
        valuesForAggErrTable.put(DATETIME_ID, timestamp);
        insertRow(table, valuesForAggErrTable);
    }

    private void insertRowIntoRawTable(final int imsi, final String timestamp) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(TAC, SAMPLE_TAC);
        valuesForRawTable.put(IMSI, imsi);
        valuesForRawTable.put(DATETIME_ID, timestamp);
        valuesForRawTable.putAll(QCIColumns.getDefaultQCIValues());
        valuesForRawTable.put(QCI_ERR_1, 1);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForRawTable);
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columns = getColumnsForAggregationTables();
        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, columns);
    }

    private Collection<String> getColumnsForRawTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(TAC);
        columns.add(IMSI);
        columns.add(DATETIME_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }

    private Collection<String> getColumnsForAggregationTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(TAC);
        columns.add(DATETIME_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_TAC);
        values.put(GROUP_NAME, SAMPLE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);

    }

}
