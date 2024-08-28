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
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedTablesCauseCodeTest extends
        BaseDataIntegrityTest<MultipleCauseCodeRankingResult> {

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G = "GTP";

    private static final String LTE_ERR = "#EVENT_E_LTE_EVNTSRC_CC_ERR_DAY";

    private static final String SGEH_ERR = "#EVENT_E_SGEH_EVNTSRC_CC_ERR_DAY";

    private static final String LTE_SUC = "#EVENT_E_LTE_EVNTSRC_CC_SUC_DAY";

    private static final String SGEH_SUC = "#EVENT_E_SGEH_EVNTSRC_CC_SUC_DAY";

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G = "NAS";

    private final static List<String> tempTables = new ArrayList<String>();

    private final int noSuccessesInSgehTable = 3;

    private final static int secondMostFrequentCauseCode_38 = 38;

    private final static int mostFrequentCauseCode_7 = 7;

    private final int causeProtTypeUsedInTable = 1;

    private final static Map<Integer, String> causeCodeMapping = new HashMap<Integer, String>();

    private final int noErrorsForMostFrequentSgehCauseCode = 11;

    private final int noErrorsForSecondMostFrequentCauseCode = 8;

    private final int noErrorsForThirdMostFrequentSgehCauseCode = 6;

    private final int noErrorsForFourthMostFrequentLteCauseCode = 2;

    private final int noSuccessesInLteTable = 17;

    static {

        causeCodeMapping.put(mostFrequentCauseCode_7, "GPRS service not allowed");
        causeCodeMapping.put(secondMostFrequentCauseCode_38, "Network Failure");
        tempTables.add(SGEH_SUC);
        tempTables.add(SGEH_ERR);
        tempTables.add(LTE_SUC);
        tempTables.add(LTE_ERR);
    }

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(multipleRankingService);

        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable);
        }
        populateTemporaryTables();
        createTemporaryTables();
    }

    @Test
    public void testGetRankingData_CauseCode() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_CAUSE_CODE);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingService, map);
        System.out.println(json);
        final List<MultipleCauseCodeRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleCauseCodeRankingResult.class);
        assertThat(rankingResult.size(), is(4));

        final MultipleCauseCodeRankingResult mostFrequentCauseCodeInRanking = rankingResult.get(0);
        assertThat(mostFrequentCauseCodeInRanking.getCauseCodeDesc(), is(causeCodeMapping.get(mostFrequentCauseCode_7)));
        assertThat(mostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(mostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(mostFrequentCauseCodeInRanking.getCauseCodeID(), is(mostFrequentCauseCode_7));
        assertThat(mostFrequentCauseCodeInRanking.getNoErrors(),
                is(Integer.toString(noErrorsForMostFrequentSgehCauseCode)));
        assertThat(mostFrequentCauseCodeInRanking.getNoSuccesses(), is(Integer.toString(noSuccessesInSgehTable)));

        final MultipleCauseCodeRankingResult secondMostFrequentCauseCodeInRanking = rankingResult.get(1);
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseCodeDesc(),
                is(causeCodeMapping.get(mostFrequentCauseCode_7)));
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(secondMostFrequentCauseCodeInRanking.getCauseCodeID(), is(mostFrequentCauseCode_7));
        assertThat(secondMostFrequentCauseCodeInRanking.getNoErrors(),
                is(Integer.toString(noErrorsForSecondMostFrequentCauseCode)));
        assertThat(secondMostFrequentCauseCodeInRanking.getNoSuccesses(), is(Integer.toString(noSuccessesInLteTable)));

        final MultipleCauseCodeRankingResult thirdMostFrequentCauseCodeInRanking = rankingResult.get(2);
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseProtTypeDesc(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseCodeDesc(),
                is(causeCodeMapping.get(secondMostFrequentCauseCode_38)));
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseProtTypeID(), is(causeProtTypeUsedInTable));
        assertThat(thirdMostFrequentCauseCodeInRanking.getCauseCodeID(), is(secondMostFrequentCauseCode_38));

    }

    private void populateTemporaryTables() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();

        insertRow(SGEH_SUC, mostFrequentCauseCode_7, noSuccessesInSgehTable, 0, dateTime);
        insertRow(SGEH_SUC, secondMostFrequentCauseCode_38, noSuccessesInSgehTable, 0, dateTime);

        insertRow(LTE_SUC, mostFrequentCauseCode_7, noSuccessesInLteTable, 0, dateTime);
        insertRow(LTE_SUC, secondMostFrequentCauseCode_38, noSuccessesInLteTable, 0, dateTime);

        insertRow(SGEH_ERR, mostFrequentCauseCode_7, 0, noErrorsForMostFrequentSgehCauseCode, dateTime);
        insertRow(SGEH_ERR, secondMostFrequentCauseCode_38, 0, noErrorsForThirdMostFrequentSgehCauseCode, dateTime);

        insertRow(LTE_ERR, mostFrequentCauseCode_7, 0, noErrorsForSecondMostFrequentCauseCode, dateTime);
        insertRow(LTE_ERR, secondMostFrequentCauseCode_38, 0, noErrorsForFourthMostFrequentLteCauseCode, dateTime);

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

    private void createTemporaryTable(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(NO_OF_ERRORS);
        columns.add(NO_OF_SUCCESSES);
        columns.add(CC_SQL_NAME);
        columns.add(TYPE_CAUSE_PROT_TYPE);
        columns.add(DATETIME_ID);
        createTemporaryTable(tempTableName, columns);
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

    private void createTemporaryTables() throws Exception {
        createTemporaryCCTables();
        populateCCTables();

        createTemporaryCPTTables();
        populateCPTTables();
    }
}
