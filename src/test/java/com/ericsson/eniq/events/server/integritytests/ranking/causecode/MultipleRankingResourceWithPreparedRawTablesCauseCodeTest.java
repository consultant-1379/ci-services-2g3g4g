package com.ericsson.eniq.events.server.integritytests.ranking.causecode;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleCauseCodeRankingResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedRawTablesCauseCodeTest extends
        BaseDataIntegrityTest<MultipleCauseCodeRankingResult> {

    private final MultipleRankingService multipleRankingResource = new MultipleRankingService();

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G = "GTP";

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G = "NAS";

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static List<String> tempAggTables = new ArrayList<String>();

    private final static int mostFrequentCauseCode_7 = 7;

    private final static int secondMostFrequentCauseCode_38 = 38;

    private static int thirdMostFrequentCauseCode_17 = 17;

    private static int fourthMostFrequentCauseCode_29 = 29;

    private final int causeProtTypeUsedInTable = 1;

    private final static Map<Integer, String> causeCodeMapping = new HashMap<Integer, String>();

    private static final int SOME_TAC = 123456;

    int noSuccessesInSgehTable = 3;

    int noSuccessesInLteTable = 5;

    static {

        causeCodeMapping.put(mostFrequentCauseCode_7, "GPRS service not allowed");
        causeCodeMapping.put(secondMostFrequentCauseCode_38, "Network Failure");
        causeCodeMapping.put(thirdMostFrequentCauseCode_17, "Network failure");
        causeCodeMapping.put(fourthMostFrequentCauseCode_29, "User authentication failed");
        tempRawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        tempAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_CC_SUC_15MIN);
        tempAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_CC_SUC_15MIN);
    }

    @Before
    public void onSetUp() throws Exception {
        attachDependencies(multipleRankingResource);
        createTemporaryDimTables();
    }

    @Test
    public void testGetRankingData_CauseCode_WithDataTiering_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        setUpTempTableAndData(true, DateTimeUtilities.getDateTimeMinus25Minutes());
        final String json = getData(THIRTY_MINUTES);
        System.out.println(json);

        final List<MultipleCauseCodeRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleCauseCodeRankingResult.class);
        assertThat(rankingResult.size(), is(4));

        final MultipleCauseCodeRankingResult mostFrequentCauseCodeInRanking = rankingResult.get(0);
        assertThat(mostFrequentCauseCodeInRanking.getCauseCodeDesc(), is(causeCodeMapping.get(mostFrequentCauseCode_7)));
        assertThat(mostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(mostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(mostFrequentCauseCodeInRanking.getCauseCodeID(), is(mostFrequentCauseCode_7));
        assertThat(mostFrequentCauseCodeInRanking.getNoErrors(), is("4"));
        assertThat(mostFrequentCauseCodeInRanking.getNoSuccesses(), is("3"));

        final MultipleCauseCodeRankingResult secondMostFrequentCauseCodeInRanking = rankingResult.get(1);
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseCodeDesc(),
                is(causeCodeMapping.get(secondMostFrequentCauseCode_38)));
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseCodeID(), is(secondMostFrequentCauseCode_38));
        assertThat(secondMostFrequentCauseCodeInRanking.getNoErrors(), is("3"));
        assertThat(secondMostFrequentCauseCodeInRanking.getNoSuccesses(), is("5"));

        final MultipleCauseCodeRankingResult thirdMostFrequentCauseCodeInRanking = rankingResult.get(2);
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseCodeDesc(),
                is(causeCodeMapping.get(thirdMostFrequentCauseCode_17)));
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseCodeID(), is(thirdMostFrequentCauseCode_17));
        assertThat(thirdMostFrequentCauseCodeInRanking.getNoErrors(), is("2"));
        assertThat(thirdMostFrequentCauseCodeInRanking.getNoSuccesses(), is("0"));

        final MultipleCauseCodeRankingResult fourthMostFrequentCauseCodeInRanking = rankingResult.get(3);
        assertThat(fourthMostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(fourthMostFrequentCauseCodeInRanking.getCauseCodeDesc(),
                is(causeCodeMapping.get(fourthMostFrequentCauseCode_29)));
        assertThat(fourthMostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(fourthMostFrequentCauseCodeInRanking.getCauseCodeID(), is(fourthMostFrequentCauseCode_29));
        assertThat(fourthMostFrequentCauseCodeInRanking.getNoErrors(), is("1"));
        assertThat(fourthMostFrequentCauseCodeInRanking.getNoSuccesses(), is("0"));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void populateTemporaryTables(final String dateTime) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, mostFrequentCauseCode_7, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, mostFrequentCauseCode_7, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, mostFrequentCauseCode_7, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, mostFrequentCauseCode_7, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, mostFrequentCauseCode_7, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, secondMostFrequentCauseCode_38, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, secondMostFrequentCauseCode_38, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, secondMostFrequentCauseCode_38, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, secondMostFrequentCauseCode_38, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, thirdMostFrequentCauseCode_17, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, thirdMostFrequentCauseCode_17, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, thirdMostFrequentCauseCode_17, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, fourthMostFrequentCauseCode_29, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, fourthMostFrequentCauseCode_29, sqlExecutor, dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final int causeCode, final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + causeCode + "," + causeProtTypeUsedInTable
                + "," + SOME_TAC + ",'" + dateTime + "')");
    }

    private void createTemporaryTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(CAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, TAC int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTemporaryCCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(CAUSE_CODE_COLUMN);
        columnsForTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForTable.add(CAUSE_CODE_DESC_COLUMN);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_SGEH_CAUSECODE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_LTE_CAUSECODE, columnsForTable);
    }

    private void createTemporaryCPTTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForTable.add(CAUSE_PROT_TYPE_DESC_COLUMN);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsForTable);
    }

    private void insertRowIntoCCTable(final String table, final int causeCode, final int causeProtoType,
            final String ccDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_CODE_COLUMN, causeCode);
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_CODE_DESC_COLUMN, ccDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoCPTTable(final String table, final int causeProtoType, final String cptDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, cptDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void populateCCTables() throws SQLException {
        for (final int causeCode : causeCodeMapping.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_SGEH_CAUSECODE, causeCode, causeProtTypeUsedInTable,
                    causeCodeMapping.get(causeCode));
        }

        for (final int causeCode : causeCodeMapping.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_LTE_CAUSECODE, causeCode, causeProtTypeUsedInTable,
                    causeCodeMapping.get(causeCode));
        }
    }

    private void populateCPTTables() throws SQLException {
        insertRowIntoCPTTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, causeProtTypeUsedInTable,
                DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G);

        insertRowIntoCPTTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, causeProtTypeUsedInTable,
                DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G);
    }

    private void createTemporaryDimTables() throws Exception {
        createTemporaryCCTables();
        populateCCTables();

        createTemporaryCPTTables();
        populateCPTTables();
    }

    private String getData(final String time) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_CAUSE_CODE);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");

        return getData(multipleRankingResource, map);
    }

    private void populateTemporarySucTables(final String dateTime) throws SQLException {
        insertRow(TEMP_EVENT_E_SGEH_EVNTSRC_CC_SUC_15MIN, mostFrequentCauseCode_7, noSuccessesInSgehTable, 0, dateTime);
        insertRow(TEMP_EVENT_E_LTE_EVNTSRC_CC_SUC_15MIN, secondMostFrequentCauseCode_38, noSuccessesInLteTable, 0,
                dateTime);
    }

    private void insertRow(final String table, final int causeCode, final int numSuccesses, final int numErrors,
            final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(CC_SQL_NAME, causeCode);
        values.put(TYPE_CAUSE_PROT_TYPE, causeProtTypeUsedInTable);
        values.put(NO_OF_SUCCESSES, numSuccesses);
        values.put(NO_OF_ERRORS, numErrors);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

    private void setUpTempTableAndData(final boolean isDataTiered, final String timeToPoulateData) throws SQLException,
            Exception {
        for (final String tempTable : tempRawTables) {
            createTemporaryTable(tempTable);
        }

        populateTemporaryTables(timeToPoulateData);

        if (isDataTiered) {
            for (final String tempSucTable : tempAggTables) {
                final Collection<String> columns = new ArrayList<String>();
                columns.add(NO_OF_ERRORS);
                columns.add(NO_OF_SUCCESSES);
                columns.add(CC_SQL_NAME);
                columns.add(TYPE_CAUSE_PROT_TYPE);
                columns.add(DATETIME_ID);
                createTemporaryTable(tempSucTable, columns);
            }
            populateTemporarySucTables(timeToPoulateData);
        }
    }

}
