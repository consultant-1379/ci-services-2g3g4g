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
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisResourceDrilldownAggTest extends TestsWithTemporaryTablesBaseTestCase<CauseCodeAnalysisDrilldownResult> {

    private CauseCodeAnalysisResource causeCodeAnalysisResource;

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G = "GTP";

    private static final String LTE_ERR = "#EVENT_E_LTE_EVNTSRC_CC_SCC_ERR_15MIN";

    private static final String LTE_ERR_RAW = "#EVENT_E_LTE_ERR_RAW";

    private static final String SGEH_ERR = "#EVENT_E_SGEH_EVNTSRC_CC_SCC_ERR_15MIN";

    private static final String SGEH_ERR_RAW = "#EVENT_E_SGEH_ERR_RAW";

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G = "NAS";

    private final static List<String> tempTopologyTables = new ArrayList<String>();

    private final static List<String> tempTables = new ArrayList<String>();

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static int causeCode_38 = 38;

    private final static int subCauseCode_12 = 12;

    private final static int subCauseCode_42 = 42;

    private final static int causeCode_7 = 7;

    private final int causeProtType = 1;

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

    static {
        causeCodeMapping.put(causeCode_7, "GPRS service not allowed");
        causeCodeMapping.put(causeCode_38, "Network Failure");

        lteCauseCodeMapping.put(causeCode_38,
                "This cause is used by the network to indicate that the requested service was rejected due to an error situation in the network.");

        subCauseCodeMapping.put(subCauseCode_12, "Auth failed: unknown subscriber; operator determined barring");
        subCauseCodeMapping.put(subCauseCode_42, "GGSN responded with reject cause different from #192 during SGSN initiated modification");

        subCauseCodeHelpMapping.put(subCauseCode_12, "");
        subCauseCodeHelpMapping.put(subCauseCode_42,
                "Capture the GTP-C traffic towards the GGSN with ITC, and analyze the protocol content. Continue troubleshooting in the GGSN.");

        lteSubCauseCodeMapping.put(subCauseCode_12, "Auth failed: unknown subscriber, operator determined barring");
        lteSubCauseCodeMapping.put(subCauseCode_42, "GGSN responded with reject cause different from #192 during SGSN initiated modification");

        lteSubCauseCodeHelpMapping.put(subCauseCode_12, "");
        lteSubCauseCodeHelpMapping.put(subCauseCode_42, "");

        tempTables.add(SGEH_ERR);
        tempTables.add(LTE_ERR);
        tempRawTables.add(SGEH_ERR_RAW);
        tempRawTables.add(LTE_ERR_RAW);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        causeCodeAnalysisResource = new CauseCodeAnalysisResource();
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
    public void testGetDrilldownData_CauseCode() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(causeCode_38));
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        validateResult(json);

    }

    private void validateResult(final String json) throws Exception {
        final List<CauseCodeAnalysisDrilldownResult> drilldownResult = getTranslator().translateResult(json, CauseCodeAnalysisDrilldownResult.class);
        assertThat(drilldownResult.size(), is(2));

        final CauseCodeAnalysisDrilldownResult firstResult = drilldownResult.get(0);

        assertThat(firstResult.getCauseProtocolType(), is(Integer.toString(causeProtType)));
        assertThat(firstResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
        assertThat(firstResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(firstResult.getCauseCode(), is(causeCodeMapping.get(causeCode_38)));
        assertThat(firstResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_42)));
        assertThat(firstResult.getSubCauseCode(), is(subCauseCodeMapping.get(subCauseCode_42)));
        assertThat(firstResult.getSubCauseCodeHelp(), is(subCauseCodeHelpMapping.get(subCauseCode_42)));
        assertThat(firstResult.getOccurrences(), is("6"));
        assertThat(firstResult.getImpactedSubscribers(), is("1"));

        final CauseCodeAnalysisDrilldownResult secondResult = drilldownResult.get(1);

        assertThat(secondResult.getCauseProtocolType(), is(Integer.toString(causeProtType)));
        assertThat(secondResult.getCauseProtocolTypeDescription(), is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
        assertThat(secondResult.getCauseCodeID(), is(Integer.toString(causeCode_38)));
        assertThat(secondResult.getCauseCode(), is(lteCauseCodeMapping.get(causeCode_38)));
        assertThat(secondResult.getSubCauseCodeID(), is(Integer.toString(subCauseCode_12)));
        assertThat(secondResult.getSubCauseCode(), is(lteSubCauseCodeMapping.get(subCauseCode_12)));
        assertThat(secondResult.getSubCauseCodeHelp(), is(lteSubCauseCodeHelpMapping.get(subCauseCode_12)));
        assertThat(secondResult.getOccurrences(), is("2"));
        assertThat(secondResult.getImpactedSubscribers(), is("1"));

    }

    private void populateTemporaryTables() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -320);

        insertRawRow(SGEH_ERR_RAW, 1, causeCode_7, subCauseCode_42, IMSI_1, dateTime);
        insertRawRow(SGEH_ERR_RAW, 1, causeCode_38, subCauseCode_42, IMSI_2, dateTime);

        insertRawRow(LTE_ERR_RAW, 1, causeCode_7, subCauseCode_42, IMSI_2, dateTime);
        insertRawRow(LTE_ERR_RAW, 1, causeCode_38, subCauseCode_12, IMSI_1, dateTime);

        insertRow(SGEH_ERR, causeCode_7, subCauseCode_12, 0, noErrorsForSgehCauseCode_7, dateTime);
        insertRow(SGEH_ERR, causeCode_38, subCauseCode_42, 0, noErrorsForSgehCauseCode_38, dateTime);

        insertRow(LTE_ERR, causeCode_7, subCauseCode_42, 0, noErrorsForLteCauseCode_7, dateTime);
        insertRow(LTE_ERR, causeCode_38, subCauseCode_12, 0, noErrorsForLteCauseCode_38, dateTime);

    }

    private void insertRow(final String table, final int causeCode, final int subCauseCode, final int numSuccesses, final int numErrors,
                           final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(CAUSE_CODE_COLUMN, causeCode);
        values.put(SUBCAUSE_CODE_COLUMN, subCauseCode);
        values.put(CAUSE_PROT_TYPE_COLUMN, causeProtType);
        values.put(NO_OF_SUCCESSES, numSuccesses);
        values.put(NO_OF_ERRORS, numErrors);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

    private void insertRawRow(final String table, final int causeProtType, final int causeCode, final int subCauseCode, final int testImsi,
                              final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(CAUSE_PROT_TYPE_COLUMN, causeProtType);
        values.put(CAUSE_CODE_COLUMN, causeCode);
        values.put(SUBCAUSE_CODE_COLUMN, subCauseCode);
        values.put(IMSI, testImsi);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

    private void createTopologyTemporaryTable(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(CAUSE_CODE_COLUMN);
        columns.add(SUBCAUSE_CODE_COLUMN);
        columns.add(ADVICE_COLUMN);
        createTemporaryTable(tempTableName, columns);
    }

    private void createTemporaryTable(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(CAUSE_CODE_COLUMN);
        columns.add(SUBCAUSE_CODE_COLUMN);
        columns.add(CAUSE_PROT_TYPE_COLUMN);
        columns.add(NO_OF_SUCCESSES);
        columns.add(NO_OF_ERRORS);
        columns.add(DATETIME_ID);
        createTemporaryTable(tempTableName, columns);
    }

    private void createRawTemporaryTable(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(CAUSE_PROT_TYPE_COLUMN);
        columns.add(CAUSE_CODE_COLUMN);
        columns.add(SUBCAUSE_CODE_COLUMN);
        columns.add(IMSI);
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
            insertRowIntoCCTable(TEMP_DIM_E_SGEH_CAUSECODE, causeCode, causeProtType, causeCodeMapping.get(causeCode));
        }

        for (final int causeCode : lteCauseCodeMapping.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_LTE_CAUSECODE, causeCode, causeProtType, lteCauseCodeMapping.get(causeCode));
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
        insertRowIntoCPTTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, causeProtType, DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G);

        insertRowIntoCPTTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, causeProtType, DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G);
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
