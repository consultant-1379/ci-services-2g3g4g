/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.integritytests.causecode.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.CauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisResourceSummaryAggTest extends TestsWithTemporaryTablesBaseTestCase<CauseCodeAnalysisSummaryResult> {

    private static final String LESS_THAN_ONE_WEEK = "10000";

    private final CauseCodeAnalysisResource causeCodeAnalysisResource = new CauseCodeAnalysisResource();

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G = "GTP";

    private static final String LTE_ERR_APN = "#EVENT_E_LTE_APN_CC_SCC_ERR_DAY";

    private static final String LTE_ERR_RAW = "#EVENT_E_LTE_ERR_RAW";

    private static final String SGEH_ERR_APN = "#EVENT_E_SGEH_APN_CC_SCC_ERR_DAY";

    private static final String SGEH_ERR_RAW = "#EVENT_E_SGEH_ERR_RAW";

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G = "NAS";

    private final static List<String> tempTopologyTables = new ArrayList<String>();

    private final static List<String> tempTables = new ArrayList<String>();

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static int causeCode_38 = 38;

    private final static int subCauseCode_12 = 12;

    private final static int subCauseCode_42 = 42;

    private final static int subCauseCode_318 = 318;

    private final static int causeCode_7 = 7;

    private final static int subCauseCode_133 = 133;

    private final int causeProtTypeUsedInTable = 1;

    private final static Map<Integer, String> causeCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteSubCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteSubCauseCodeHelpMapping = new HashMap<Integer, String>();

    private final int noErrorsForSgehCauseCode_7 = 11;

    private final int noErrorsForLteCauseCode_7 = 8;

    private final int noErrorsForSgehCauseCode_38 = 6;

    private final int noErrorsForLteCauseCode_38 = 2;

    private static final int IMSI_1 = 123456;

    private static final int IMSI_2 = 123455;

    private static final String APN_1 = "blackberry.net";

    static {

        causeCodeMapping.put(causeCode_7, "GPRS service not allowed");
        causeCodeMapping.put(causeCode_38, "Network Failure");

        lteCauseCodeMapping.put(causeCode_38, "This cause is used by the network to indicate that the requested service was rejected.");
        lteCauseCodeMapping.put(causeCode_7, "This cause code is sent to the #UE# when there is no #EPS# subscription in the #HSS# for this #IMSI#.");

        subCauseCodeMapping.put(subCauseCode_12, "Auth failed: unknown subscriber; operator determined barring");
        subCauseCodeMapping.put(subCauseCode_42, "GGSN responded with reject cause different from SGSN initiated modification");

        subCauseCodeHelpMapping.put(subCauseCode_12, "");
        subCauseCodeHelpMapping.put(subCauseCode_42,
                "Capture the GTP-C traffic towards the GGSN with ITC, and analyze the protocol content. Continue troubleshooting in the GGSN.");

        lteSubCauseCodeMapping.put(subCauseCode_133, "GTP: sending of message towards SGSN failed");
        lteSubCauseCodeMapping.put(subCauseCode_318, "Get GGSN address failed");

        lteSubCauseCodeHelpMapping.put(subCauseCode_133, "");
        lteSubCauseCodeHelpMapping.put(subCauseCode_318, "");

        tempTables.add(SGEH_ERR_APN);
        tempTables.add(LTE_ERR_APN);
        tempRawTables.add(SGEH_ERR_RAW);
        tempRawTables.add(LTE_ERR_RAW);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources. TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        attachDependencies(causeCodeAnalysisResource);

        for (final String tempTopologyTable : tempTopologyTables) {
            createTopologyTemporaryTable(tempTopologyTable);
        }

        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable);
        }

        for (final String tempRawTable : tempRawTables) {
            createRawTemporaryTable(tempRawTable);
        }

        populateTemporaryTables();
        createTemporaryTables();
    }

    @Test
    public void testGetSummaryData_CauseCode_APN() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, LESS_THAN_ONE_WEEK);
        map.putSingle(NODE_PARAM, APN_1);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        final List<CauseCodeAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, CauseCodeAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        final CauseCodeAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getNode(), is(APN_1));
        assertThat(firstResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(firstResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(firstResult.getCauseCodeID(), is(Integer.toString(causeCode_7)));
        assertThat(firstResult.getCauseCode(), is(causeCodeMapping.get(causeCode_7)));
        assertThat(firstResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_12)));
        assertThat(firstResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_12)));
        assertThat(firstResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_12)));
        assertThat(firstResult.getOccurrences(), is("1"));
        assertThat(firstResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getNode(), is(APN_1));
        assertThat(secondResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(secondResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(secondResult.getCauseCodeID(), is(Integer.toString(causeCode_7)));
        assertThat(secondResult.getCauseCode(), is(lteCauseCodeMapping.get(causeCode_7)));
        assertThat(secondResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_133)));
        assertThat(secondResult.getSubCauseCode(), is(lteSubCauseCodeMapping.get(subCauseCode_133)));
        assertThat(secondResult.getSubCauseCodeHelp(), is(lteSubCauseCodeHelpMapping.get(subCauseCode_133)));
        assertThat(secondResult.getOccurrences(), is("1"));
        assertThat(secondResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult thrirdResult = summaryResult.get(2);
        assertThat(thrirdResult.getNode(), is(APN_1));
        assertThat(thrirdResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(thrirdResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(thrirdResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(thrirdResult.getCauseCode(), is(causeCodeMapping.get(causeCode_38)));
        assertThat(thrirdResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_42)));
        assertThat(thrirdResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_42)));
        assertThat(thrirdResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_42)));
        assertThat(thrirdResult.getOccurrences(), is("1"));
        assertThat(thrirdResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult fourthResult = summaryResult.get(3);
        assertThat(fourthResult.getNode(), is(APN_1));
        assertThat(fourthResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(fourthResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(fourthResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(fourthResult.getCauseCode(), is(lteCauseCodeMapping.get(causeCode_38)));
        assertThat(fourthResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_318)));
        assertThat(fourthResult.getSubCauseCode(), is(lteSubCauseCodeMapping.get(subCauseCode_318)));
        assertThat(fourthResult.getSubCauseCodeHelp(), is(lteSubCauseCodeHelpMapping.get(subCauseCode_318)));
        assertThat(fourthResult.getOccurrences(), is("1"));
        assertThat(fourthResult.getImpactedSubscribers(), is("1"));

    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -3200);

            insertRawRow(SGEH_ERR_RAW, causeCode_7, subCauseCode_12, IMSI_1, APN_1, SAMPLE_TAC, sqlExecutor, dateTime);
            insertRawRow(SGEH_ERR_RAW, causeCode_38, subCauseCode_42, IMSI_2, APN_1, SAMPLE_TAC, sqlExecutor, dateTime);

            insertRawRow(LTE_ERR_RAW, causeCode_7, subCauseCode_133, IMSI_2, APN_1, SAMPLE_TAC, sqlExecutor, dateTime);
            insertRawRow(LTE_ERR_RAW, causeCode_38, subCauseCode_318, IMSI_1, APN_1, SAMPLE_TAC, sqlExecutor, dateTime);

            insertRow(SGEH_ERR_APN, causeCode_7, subCauseCode_12, 0, noErrorsForSgehCauseCode_7, APN_1, sqlExecutor, dateTime);
            insertRow(SGEH_ERR_APN, causeCode_38, subCauseCode_42, 0, noErrorsForSgehCauseCode_38, APN_1, sqlExecutor, dateTime);

            insertRow(LTE_ERR_APN, causeCode_7, subCauseCode_133, 0, noErrorsForLteCauseCode_7, APN_1, sqlExecutor, dateTime);

            insertRow(LTE_ERR_APN, causeCode_38, subCauseCode_318, 0, noErrorsForLteCauseCode_38, APN_1, sqlExecutor, dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final int causeCode, final int subCauseCode, final int numSuccesses, final int numErrors,
                           final String testNode, final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + causeCode + "," + subCauseCode + "," + causeProtTypeUsedInTable + ","
                + numSuccesses + "," + numErrors + ",'" + testNode + "','" + dateTime + "')");
    }

    private void insertRawRow(final String table, final int causeCode, final int subCauseCode, final int testImsi, final String testNode,
                              final int tac, final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + causeCode + "," + subCauseCode + "," + causeProtTypeUsedInTable + ","
                + testImsi + ",'" + testNode + "','" + tac + "','" + dateTime + "')");
    }

    private void createTopologyTemporaryTable(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(CAUSE_CODE_COLUMN);
        columns.add(SUBCAUSE_CODE_COLUMN);
        columns.add(ADVICE_COLUMN);
        createTemporaryTable(tempTableName, columns);
    }

    private void createTemporaryTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, NO_OF_SUCCESSES int, "
                    + "NO_OF_ERRORS int, APN varchar(128), DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createRawTemporaryTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, IMSI int, APN varchar(128), TAC unsigned int, "
                    + "DATETIME_ID timestamp)");
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

    private void createTemporarySCCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(SUBCAUSE_CODE_COLUMN);
        columnsForTable.add(SUB_CAUSE_CODE_DESC_COLUMN);
        columnsForTable.add(SUB_CAUSE_CODE_HELP_COLUMN);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsForTable);
    }

    private void createTemporaryCPTTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForTable.add(CAUSE_PROT_TYPE_DESC_COLUMN);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsForTable);
    }

    private void insertTopologyRow(final String table, final int causeCode, final int subCauseCode, String advice) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(CAUSE_CODE_COLUMN, causeCode);
        values.put(SUBCAUSE_CODE_COLUMN, subCauseCode);
        values.put(ADVICE_COLUMN, advice);
        insertRow(table, values);
    }

    private void insertRowIntoCCTable(final String table, final int causeCode, final int causeProtoType, final String ccDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_CODE_COLUMN, causeCode);
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_CODE_DESC_COLUMN, ccDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoSCCTable(final String table, final int subCauseCode, final String sccDesc, final String whatNext) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(SUBCAUSE_CODE_COLUMN, subCauseCode);
        valuesForTable.put(SUB_CAUSE_CODE_DESC_COLUMN, sccDesc);
        valuesForTable.put(SUB_CAUSE_CODE_HELP_COLUMN, whatNext);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoCPTTable(final String table, final int causeProtoType, final String cptDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, cptDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void populateCCTables() throws SQLException {
        for (final int causeCode : causeCodeMapping.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_SGEH_CAUSECODE, causeCode, causeProtTypeUsedInTable, causeCodeMapping.get(causeCode));
        }

        for (final int causeCode : lteCauseCodeMapping.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_LTE_CAUSECODE, causeCode, causeProtTypeUsedInTable, lteCauseCodeMapping.get(causeCode));
        }
    }

    private void populateSCCTables() throws SQLException {
        for (final int subcauseCode : subCauseCodeMapping.keySet()) {
            insertRowIntoSCCTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, subcauseCode, subCauseCodeMapping.get(subcauseCode),
                    subCauseCodeHelpMapping.get(subcauseCode));
        }

        for (final int subcauseCode : lteSubCauseCodeMapping.keySet()) {
            insertRowIntoSCCTable(TEMP_DIM_E_LTE_SUBCAUSECODE, subcauseCode, lteSubCauseCodeMapping.get(subcauseCode),
                    lteSubCauseCodeHelpMapping.get(subcauseCode));
        }
    }

    private void populateCPTTables() throws SQLException {
        insertRowIntoCPTTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, causeProtTypeUsedInTable, DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G);

        insertRowIntoCPTTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, causeProtTypeUsedInTable, DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G);
    }

    private void createTemporaryTables() throws Exception {
        createTemporaryCCTables();
        populateCCTables();

        createTemporarySCCTables();
        populateSCCTables();

        createTemporaryCPTTables();
        populateCPTTables();
    }
}
