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
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeAnalysisDrilldownResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisResourceDrilldownRawTest extends TestsWithTemporaryTablesBaseTestCase<CauseCodeAnalysisDrilldownResult> {

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

    // only applicable for LTE
    private final static int subCauseCode_522 = 522;

    private final static Map<Integer, String> causeCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> causeCodeMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMappingLTE = new HashMap<Integer, String>();

    private static final int IMSI_1 = 123456;

    private static final int IMSI_2 = 123455;

    private static final int TAC_1 = 1234567;

    static {
        // Populate SGEH cause codes/descriptions
        causeCodeMapping.put(causeCode_7, "GPRS service not allowed");
        causeCodeMapping.put(causeCode_38, "Network Failure");
        causeCodeMapping.put(causeCode_17, "Network failure");
        causeCodeMapping.put(causeCode_29, "User authentication failed");

        // Populate LTE cause codes/descriptions
        causeCodeMappingLTE.put(causeCode_38,
                "This cause is used by the network to indicate that the requested service was rejected due to an error situation in the network.");

        // SGEH sub cause codes/descriptions
        subCauseCodeMapping.put(subCauseCode_12, "Auth failed: unknown subscriber; operator determined barring");
        subCauseCodeMapping.put(subCauseCode_42, "GGSN responded with reject cause different from #192 during SGSN initiated modification");
        subCauseCodeMapping.put(subCauseCode_56, "XID Negotioation");

        subCauseCodeHelpMapping.put(subCauseCode_12, "");
        subCauseCodeHelpMapping.put(subCauseCode_42,
                "Capture the GTP-C traffic towards the GGSN with ITC, and analyze the protocol content. Continue troubleshooting in the GGSN.");
        subCauseCodeHelpMapping.put(subCauseCode_56, "");

        // LTE sub cause codes/descriptions
        subCauseCodeMappingLTE.put(subCauseCode_522, "Used in the Dedicated bearer activation procedure, for CC #68, service not supported.");

        subCauseCodeHelpMappingLTE.put(subCauseCode_522, "");

        tempTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
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
    public void testGetRankingData_CauseCode_5Minutes() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(causeCode_38));
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();

        System.out.println(json);
        final List<CauseCodeAnalysisDrilldownResult> drilldownResult = getTranslator().translateResult(json, CauseCodeAnalysisDrilldownResult.class);
        assertThat(drilldownResult.size(), is(4));

        final CauseCodeAnalysisDrilldownResult firstResult = drilldownResult.get(0);
        assertThat(firstResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(firstResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(firstResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(firstResult.getCauseCode(), is(causeCodeMapping.get(causeCode_38)));
        assertThat(firstResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_12)));
        assertThat(firstResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_12)));
        assertThat(firstResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_12)));
        assertThat(firstResult.getOccurrences(), is("3"));
        assertThat(firstResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisDrilldownResult secondResult = drilldownResult.get(1);
        assertThat(secondResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(secondResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(secondResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(secondResult.getCauseCode(), is(causeCodeMapping.get(causeCode_38)));
        assertThat(secondResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_42)));
        assertThat(secondResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_42)));
        assertThat(secondResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_42)));
        assertThat(secondResult.getOccurrences(), is("2"));
        assertThat(secondResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisDrilldownResult thirdResult = drilldownResult.get(2);
        assertThat(thirdResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(thirdResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(thirdResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(thirdResult.getCauseCode(), is(causeCodeMapping.get(causeCode_38)));
        assertThat(thirdResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_56)));
        assertThat(thirdResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_56)));
        assertThat(thirdResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_56)));
        assertThat(thirdResult.getOccurrences(), is("1"));
        assertThat(thirdResult.getImpactedSubscribers(), is("1"));

        // the LTE result
        final CauseCodeAnalysisDrilldownResult fourthResult = drilldownResult.get(3);
        assertThat(fourthResult.getCauseProtocolType(), is(Integer.toString(causeProtTypeUsedInTable)));
        assertThat(fourthResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(fourthResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(fourthResult.getCauseCode(), is(causeCodeMappingLTE.get(causeCode_38)));
        assertThat(fourthResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_522)));
        assertThat(fourthResult.getSubCauseCode(), is(subCauseCodeMappingLTE.get(subCauseCode_522)));
        // as DIM_E_LTE_CC_SCC table doesnt exist yet query will return "" as advice for all LTE
        assertThat(fourthResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMappingLTE.get(subCauseCode_522)));
        assertThat(fourthResult.getOccurrences(), is("2"));
        assertThat(fourthResult.getImpactedSubscribers(), is("1"));

    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_1, TAC_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_56, IMSI_1, TAC_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_1, TAC_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_1, TAC_1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_42, IMSI_1, TAC_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, causeCode_38, subCauseCode_42, IMSI_1, TAC_1, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_522, IMSI_2, TAC_1, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, causeCode_38, subCauseCode_522, IMSI_2, TAC_1, sqlExecutor, dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final int causeCode, final int subCauseCode, final int testImsi, final int testTac,
                           final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + causeCode + "," + subCauseCode + "," + causeProtTypeUsedInTable + ","
                + testImsi + "," + testTac + ",'" + dateTime + "')");
    }

    private void createTemporaryTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, IMSI int, TAC int, " + "DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTopologyTemporaryTable(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(CAUSE_CODE_COLUMN);
        columns.add(SUBCAUSE_CODE_COLUMN);
        columns.add(ADVICE_COLUMN);
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