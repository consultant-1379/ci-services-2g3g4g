/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
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

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.KEY_TYPE_ERR;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.NODE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_REQUEST_ID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TWO_WEEKS;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */

public abstract class BaseQOSStatisticsResourceWithPreparedTablesTest extends
        TestsWithTemporaryTablesBaseTestCase<QOSStatisticsSummaryResult> {

    private static final String QCI_SUC_ = "QCI_SUC_";

    private static final String QCI_ERR_ = "QCI_ERR_";

    protected static final int SOME_TAC = 123456;

    protected static List<QCIData> qciDataList;

    protected QOSStatisticsResource qciResource;

    protected List<String> aggTables = new ArrayList<String>();

    private static List<String> rawTables = new ArrayList<String>();

    static {
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        qciResource = new QOSStatisticsResource();
        attachDependencies(qciResource);

    }

    void getQCISummary(final String type, final String node, final String time) throws Exception {
        final int time1 = Integer.parseInt(time) - 2;
        final int time2 = Integer.parseInt(time) - 4;
        createRawTables();
        createAggregationTables();
        populateTablesForAggregationQuery(DateTimeUtilities.getDateTime(Calendar.MINUTE, -time1),
                DateTimeUtilities.getDateTime(Calendar.MINUTE, -time2));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(NODE_PARAM, node);
        getQCISummary(map, type, time);
    }

    void getQCISummaryForTwoWeeks(final String type, final String node) throws Exception {
        createAggregationTables();
        createRawTables();
        populateTablesForAggregationQuery(DateTimeUtilities.getDateTimeMinus48Hours(),
                DateTimeUtilities.getDateTimeMinus36Hours());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(NODE_PARAM, node);
        getQCISummary(map, type, TWO_WEEKS);

    }

    private void populateTablesForAggregationQuery(final String firstTimeStamp, final String secondTimeStamp)
            throws SQLException {
        populateTables(aggTables, firstTimeStamp, secondTimeStamp);
        populateTables(rawTables, firstTimeStamp, secondTimeStamp);
    }

    void getQCIGroupSummary(final String type, final String group, final String time) throws Exception {
        final int time1 = Integer.parseInt(time) - 2;
        final int time2 = Integer.parseInt(time) - 4;
        createRawTables();
        createAggregationTables();
        populateTablesForAggregationQuery(DateTimeUtilities.getDateTime(Calendar.MINUTE, -time1),
                DateTimeUtilities.getDateTime(Calendar.MINUTE, -time2));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, group);
        getQCISummary(map, type, time);

    }

    private void createAggregationTables() throws Exception {
        createTables(aggTables, getColumnsForAggregationTables());
    }

    private void createRawTables() throws Exception {
        createTables(rawTables, getColumnsForRawTables());
    }

    void getQCIGroupSummaryForFiveMinutes(final String type, final String group) throws Exception {
        createRawTables();
        populateTables(rawTables, DateTimeUtilities.getDateTime(Calendar.MINUTE, -4),
                DateTimeUtilities.getDateTime(Calendar.MINUTE, -3));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, group);
        getQCISummary(map, type, FIVE_MINUTES);

    }

    private void getQCISummary(final MultivaluedMap<String, String> map, final String type, final String time)
            throws Exception {
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "10");
        final String json = qciResource.getData(SAMPLE_REQUEST_ID, map);
        System.out.println(json);
        jsonAssertUtils.assertJSONSucceeds(json);
        final List<QOSStatisticsSummaryResult> qciSummaryResult = getTranslator().translateResult(json,
                QOSStatisticsSummaryResult.class);
        assertThat(qciSummaryResult.size(), is(qciDataList.size()));
        validateEachElementInList(qciSummaryResult);

    }

    private void validateEachElementInList(final List<QOSStatisticsSummaryResult> qciSummaryResult) {
        int index = 0;
        for (final QCIData qciData : qciDataList) {
            final QOSStatisticsSummaryResult result = qciSummaryResult.get(index++);
            compareResultWithExpectedQCIData(qciData, result);
        }
    }

    private void compareResultWithExpectedQCIData(final QCIData qciData, final QOSStatisticsSummaryResult result) {
        assertThat(result.getQCIId(), is(qciData.index));
        assertThat(result.getDescription(), is(qciData.description));
        assertThat(result.getNoErrors(), is(qciData.getTotalErrors()));
        assertThat(result.getNoSuccesses(), is(qciData.getTotalSuccesses()));
        assertThat(result.getImpactedSubscribers(), is(2));
    }

    private void insertRow(final String table, final String dateTimeForFirstInsert, final int dataIndex)
            throws SQLException {
        final Map<String, Object> values = getValues(table, dateTimeForFirstInsert, dataIndex);
        insertRow(table, values);
    }

    private Map<String, Object> getValues(final String table, final String dateTimeForFirstInsert, final int dataIndex) {
        final Map<String, Object> columnsAndValues = new HashMap<String, Object>();
        if (isRawTable(table)) {
            columnsAndValues.put(TAC, SOME_TAC);
            columnsAndValues.put(IMSI, getRandomIMSI());
        }
        columnsAndValues.putAll(getTableSpecificColumnsAndValues());

        for (final QCIData qciData : qciDataList) {
            if (table.contains(KEY_TYPE_ERR)) {
                columnsAndValues.put(QCI_ERR_ + qciData.index, qciData.errors[dataIndex]);
                columnsAndValues.put(QCI_SUC_ + qciData.index, 0);
            } else {
                columnsAndValues.put(QCI_ERR_ + qciData.index, 0);
                columnsAndValues.put(QCI_SUC_ + qciData.index, qciData.successes[dataIndex]);
            }
        }
        columnsAndValues.put(DATETIME_ID, dateTimeForFirstInsert);
        return columnsAndValues;
    }

    abstract Map<String, Object> getTableSpecificColumnsAndValues();

    private void populateTables(final List<String> tables, final String firstTimeStamp, final String secondTimeStamp)
            throws SQLException {
        for (final String table : tables) {
            insertRow(table, firstTimeStamp, 0);
            insertRow(table, secondTimeStamp, 1);
        }
    }

    private void createTables(final List<String> tables, final Collection<String> columns) throws Exception {
        for (final String table : tables) {
            createTemporaryTable(table, columns);
        }
    }

    private Collection<String> getColumnsForRawTables() {
        final Collection<String> columnsAndValues = new ArrayList<String>();
        columnsAndValues.add(TAC);
        columnsAndValues.add(IMSI);
        columnsAndValues.addAll(getTableSpecificColumnsAndValues().keySet());

        for (final QCIData qciData : qciDataList) {
            columnsAndValues.add(QCI_ERR_ + qciData.index);
            columnsAndValues.add(QCI_SUC_ + qciData.index);

        }
        columnsAndValues.add(DATETIME_ID);
        return columnsAndValues;

    }

    private Collection<String> getColumnsForAggregationTables() {
        final Collection<String> columnsAndValues = new ArrayList<String>();
        columnsAndValues.addAll(getTableSpecificColumnsAndValues().keySet());

        for (final QCIData qciData : qciDataList) {
            columnsAndValues.add(QCI_ERR_ + qciData.index);
            columnsAndValues.add(QCI_SUC_ + qciData.index);

        }
        columnsAndValues.add(DATETIME_ID);
        return columnsAndValues;

    }

    static {

        qciDataList = new ArrayList<QCIData>();
        qciDataList.add(new QCIData(1, "Conversational Voice", new int[] { 1, 1 }, new int[] { 2, 1 }));
        qciDataList
                .add(new QCIData(2, "Conversational Video (Live Streaming)", new int[] { 2, 2 }, new int[] { 2, 2 }));
        qciDataList.add(new QCIData(3, "Real Time Gaming", new int[] { 3, 3 }, new int[] { 3, 3 }));
        qciDataList.add(new QCIData(4, "Non-Conversational Video (Buffered Streaming)", new int[] { 1, 1 }, new int[] {
                1, 1 }));
        qciDataList.add(new QCIData(5, "IMS Signalling", new int[] { 1, 1 }, new int[] { 1, 1 }));
        qciDataList.add(new QCIData(6, "Video (Buffered Streaming) TCP-based", new int[] { 1, 1 }, new int[] { 1, 1 }));
        qciDataList.add(new QCIData(7, "Voice, Video (Live Streaming)", new int[] { 1, 1 }, new int[] { 1, 1 }));
        qciDataList.add(new QCIData(8, "Premium;Video (Buffered Streaming) TCP-based", new int[] { 1, 1 }, new int[] {
                1, 1 }));
        qciDataList.add(new QCIData(9, "Video (Buffered Streaming) TCP-based", new int[] { 1, 1 }, new int[] { 1, 1 }));
        qciDataList.add(new QCIData(10, "Others", new int[] { 4, 5 }, new int[] { 3, 2 }));
    }

    public static class QCIData {

        public String description;

        public int index;

        public int[] successes;

        public int[] errors;

        public QCIData(final int i, final String description, final int[] errors, final int[] successes) {
            index = i;
            this.description = description;
            this.errors = errors;
            this.successes = successes;
        }

        public int getTotalSuccesses() {
            int sum = 0;
            for (final int i : successes) {
                sum += i;
            }
            return sum;
        }

        public int getTotalErrors() {
            int sum = 0;
            for (final int i : errors) {
                sum += i;
            }
            return sum;
        }

    }

}
