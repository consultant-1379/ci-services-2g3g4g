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

public class CauseCodeAnalysisResourceGroupSummaryRawTest extends TestsWithTemporaryTablesBaseTestCase<CauseCodeAnalysisSummaryResult> {

    private final CauseCodeAnalysisResource causeCodeAnalysisResource = new CauseCodeAnalysisResource();

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G = "GTP";

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G = "NAS";

    private final static List<String> tempTopologyTables = new ArrayList<String>();

    private final static List<String> tempTables = new ArrayList<String>();

    private final int causeProtTypeUsedInTable = 1;

    private final static int causeCode_38 = 38;

    private final static int causeCode_17 = 17;

    private final static int causeCode_29 = 29;

    private final static int causeCode_7 = 7;

    private final static int subCauseCode_12 = 12;

    private final static int subCauseCode_56 = 56;

    private final static int subCauseCode_42 = 42;

    private final static Map<Integer, String> causeCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMapping = new HashMap<Integer, String>();

    private static final int IMSI_1 = 123456;

    private static final int IMSI_2 = 123455;

    private static final int TAC_1 = 1234567;

    private static final String APN_1 = "blackberry.net";

    private static final String GROUP_TYPE_E_APN = "#GROUP_TYPE_E_APN";

    private final static HashMap<String, String> apnGroupTableColumns;

    private final static HashMap<String, Object> valuesForGroupTable;

    // only applicable for LTE
    private final static int subCauseCode_522 = 522;

    private final static Map<Integer, String> causeCodeMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMappingLTE = new HashMap<Integer, String>();

    static {
        causeCodeMapping.put(causeCode_7, "GPRS service not allowed");
        causeCodeMapping.put(causeCode_38, "Network Failure");
        causeCodeMapping.put(causeCode_17, "Network failure");
        causeCodeMapping.put(causeCode_29, "User authentication failed");

        subCauseCodeMapping.put(subCauseCode_12, "Auth failed: unknown subscriber; operator determined barring");
        subCauseCodeMapping.put(subCauseCode_42, "GGSN responded with reject cause different from #192 during SGSN initiated modification");
        subCauseCodeMapping.put(subCauseCode_56, "XID Negotioation");

        subCauseCodeHelpMapping.put(subCauseCode_12, "Check the subscription data in the HLR.");
        subCauseCodeHelpMapping.put(subCauseCode_42, "");
        subCauseCodeHelpMapping.put(subCauseCode_56, "");

        tempTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_ERR_RAW);

        apnGroupTableColumns = new HashMap<String, String>();
        apnGroupTableColumns.put(GROUP_NAME, VARCHAR_64);
        apnGroupTableColumns.put(APN, VARCHAR_127);
        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(APN, APN_1);
        valuesForGroupTable.put(GROUP_NAME, SAMPLE_APN_GROUP);

        // Populate LTE cause codes/descriptions
        causeCodeMappingLTE.put(causeCode_38,
                "This cause is used by the network to indicate that the requested service was rejected due to an error situation in the network.");
        // LTE sub cause codes/descriptions
        subCauseCodeMappingLTE.put(subCauseCode_522, "Used in the Dedicated bearer activation procedure, for CC #68, service not supported.");
        subCauseCodeMappingLTE.put(subCauseCode_12, "Auth failed: unknown subscriber, operator determined barring");
        subCauseCodeMappingLTE.put(subCauseCode_56, "XID Negotiation");

        subCauseCodeHelpMappingLTE.put(subCauseCode_522, "Revise the Dedicated bearers feature configuration in the SGSN-MME.");
        subCauseCodeHelpMappingLTE.put(subCauseCode_12, "");
        subCauseCodeHelpMappingLTE.put(subCauseCode_56, "");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
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

        populateTemporaryTables();
        createTemporaryTables();
    }

    @Test
    public void testGetSummaryData_CauseCode_APN() throws Exception {

        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_APN, apnGroupTableColumns);
        insertRow(GROUP_TYPE_E_APN, valuesForGroupTable);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        validateResult(json);
    }

    private void validateResult(final String json) throws Exception {
        final List<CauseCodeAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, CauseCodeAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(6));

        final CauseCodeAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getNode(), is(SAMPLE_APN_GROUP));
        assertThat(firstResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(firstResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(firstResult.getCauseCodeID(), is(Integer.toString(causeCode_17)));
        assertThat(firstResult.getCauseCode(), is(causeCodeMapping.get(causeCode_17)));
        assertThat(firstResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_12)));
        assertThat(firstResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_12)));
        assertThat(firstResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_12)));
        assertThat(firstResult.getOccurrences(), is("3"));
        assertThat(firstResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getNode(), is(SAMPLE_APN_GROUP));
        assertThat(secondResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(secondResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(secondResult.getCauseCodeID(), is(Integer.toString(causeCode_29)));
        assertThat(secondResult.getCauseCode(), is(causeCodeMapping.get(causeCode_29)));
        assertThat(secondResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_42)));
        assertThat(secondResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_42)));
        assertThat(secondResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_42)));
        assertThat(secondResult.getOccurrences(), is("2"));
        assertThat(secondResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult thirdResult = summaryResult.get(3);
        assertThat(thirdResult.getNode(), is(SAMPLE_APN_GROUP));
        assertThat(thirdResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(thirdResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(thirdResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(thirdResult.getCauseCode(), is(causeCodeMappingLTE.get(causeCode_38)));
        assertThat(thirdResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_12)));
        assertThat(thirdResult.getSubCauseCode(), is(subCauseCodeMappingLTE.get(subCauseCode_12)));
        assertThat(thirdResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMappingLTE.get(subCauseCode_12)));
        assertThat(thirdResult.getOccurrences(), is("2"));
        assertThat(thirdResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult fourthResult = summaryResult.get(2);
        assertThat(fourthResult.getNode(), is(SAMPLE_APN_GROUP));
        assertThat(fourthResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(fourthResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(fourthResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(fourthResult.getCauseCode(), is(causeCodeMapping.get(causeCode_38)));
        assertThat(fourthResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_56)));
        assertThat(fourthResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_56)));
        assertThat(fourthResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_56)));
        assertThat(fourthResult.getOccurrences(), is("1"));
        assertThat(fourthResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult fifthResult = summaryResult.get(4);
        assertThat(fifthResult.getNode(), is(SAMPLE_APN_GROUP));
        assertThat(fifthResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(fifthResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(fifthResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(fifthResult.getCauseCode(), is(causeCodeMappingLTE.get(causeCode_38)));
        assertThat(fifthResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_56)));
        assertThat(fifthResult.getSubCauseCode(), is(subCauseCodeMappingLTE.get(subCauseCode_56)));
        assertThat(fifthResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMappingLTE.get(subCauseCode_56)));
        assertThat(fifthResult.getOccurrences(), is("2"));
        assertThat(fifthResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisSummaryResult sixthResult = summaryResult.get(5);
        System.out.println("sixthResult:" + sixthResult);
        assertThat(sixthResult.getNode(), is(SAMPLE_APN_GROUP));
        assertThat(sixthResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(sixthResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(sixthResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(sixthResult.getCauseCode(), is(causeCodeMappingLTE.get(causeCode_38)));
        assertThat(sixthResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_522)));
        assertThat(sixthResult.getSubCauseCode(), is(subCauseCodeMappingLTE.get(subCauseCode_522)));
        assertThat(sixthResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMappingLTE.get(subCauseCode_522)));
        assertThat(sixthResult.getOccurrences(), is("2"));
        assertThat(sixthResult.getImpactedSubscribers(), is("1"));

    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -20);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_17, subCauseCode_12, IMSI_1, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_56, IMSI_1, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_17, subCauseCode_12, IMSI_1, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_17, subCauseCode_12, IMSI_1, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_29, subCauseCode_42, IMSI_1, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_29, subCauseCode_42, IMSI_1, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_56, IMSI_2, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_56, IMSI_2, TAC_1, APN_1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_2, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_2, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_522, IMSI_2, TAC_1, APN_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_522, IMSI_2, TAC_1, APN_1, sqlExecutor, dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final int causeCode, final int subCauseCode, final int testImsi, final int testTac,
                           final String testNode, final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + causeCode + "," + subCauseCode + "," + causeProtTypeUsedInTable + ","
                + testImsi + "," + testTac + ",'" + testNode + "','" + dateTime + "')");
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
                    + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, IMSI int, TAC int, APN varchar(128), "
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

        for (final int causeCode : causeCodeMappingLTE.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_LTE_CAUSECODE, causeCode, causeProtTypeUsedInTable, causeCodeMappingLTE.get(causeCode));
        }
    }

    private void populateSCCTables() throws SQLException {
        for (final int subcauseCode : subCauseCodeMapping.keySet()) {
            insertRowIntoSCCTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, subcauseCode, subCauseCodeMapping.get(subcauseCode),
                    subCauseCodeHelpMapping.get(subcauseCode));
        }

        for (final int subcauseCode : subCauseCodeMappingLTE.keySet()) {
            insertRowIntoSCCTable(TEMP_DIM_E_LTE_SUBCAUSECODE, subcauseCode, subCauseCodeMappingLTE.get(subcauseCode),
                    subCauseCodeHelpMappingLTE.get(subcauseCode));
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
