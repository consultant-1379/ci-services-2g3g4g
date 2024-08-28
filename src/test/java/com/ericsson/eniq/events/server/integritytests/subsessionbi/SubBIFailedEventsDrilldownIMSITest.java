package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.RawTablesPopulator;
import com.ericsson.eniq.events.server.test.queryresults.SubBIFailuresDrilldownResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.ATTACH;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.COMMA;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_HANDOVER;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.NODE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.HANDOVER_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_CODE_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_CODE_DESC_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_PROT_TYPE_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_PROT_TYPE_DESC_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_LTE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_PTMSI_VALUE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SUBCAUSE_CODE_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SUB_CAUSE_CODE_DESC_COLUMN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TZ_OFFSET_OF_ZERO;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_LTE_CAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_LTE_CAUSE_PROT_TYPE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_LTE_SUBCAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_CAUSECODE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_SUBCAUSECODE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SubBIFailedEventsDrilldownIMSITest extends
        TestsWithTemporaryTablesBaseTestCase<SubBIFailuresDrilldownResult> {

    private SubsessionBIResource subsessionBIResource;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();
        attachDependencies(subsessionBIResource);
        createAndPopulateRawTables();
        createTemporaryTables();
    }

    @Test
    public void testSubBIFailedEvents_IMSI_ATTACH() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(SAMPLE_IMSI));
        map.putSingle(NODE_PARAM, ATTACH + COMMA + ATTACH_IN_2G_AND_3G);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();
        System.out.println(json);

        final List<SubBIFailuresDrilldownResult> results = getTranslator().translateResult(json,
                SubBIFailuresDrilldownResult.class);

        validateResultsFromSGEHTable(results);
    }

    @Test
    public void testSubBIFailedEvents_IMSI_L_HANDOVER() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(SAMPLE_IMSI));
        map.putSingle(NODE_PARAM, L_HANDOVER + COMMA + HANDOVER_IN_4G);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();
        System.out.println(json);

        final List<SubBIFailuresDrilldownResult> results = getTranslator().translateResult(json,
                SubBIFailuresDrilldownResult.class);

        validateResultsFromLTETable(results);
    }

    private void validateResultsFromLTETable(final List<SubBIFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final SubBIFailuresDrilldownResult activateResult = results.get(0);
        assertThat(activateResult.getIMSI(), is(SAMPLE_IMSI));
        assertThat(activateResult.getPTMSI(), is(""));

    }

    private void validateResultsFromSGEHTable(final List<SubBIFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final SubBIFailuresDrilldownResult activateResult = results.get(0);
        assertThat(activateResult.getIMSI(), is(SAMPLE_IMSI));
        assertThat(activateResult.getPTMSI(), is(Integer.toString(SAMPLE_PTMSI_VALUE)));
    }

    private void createAndPopulateRawTables() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final RawTablesPopulator rawTablesPopulator = new RawTablesPopulator();
        final Map<String, Object> valuesForSGEHTable = new HashMap<String, Object>();
        valuesForSGEHTable.put(EVENT_ID, ATTACH_IN_2G_AND_3G);
        valuesForSGEHTable.put(RAT, RAT_FOR_GSM);
        valuesForSGEHTable.put(IMSI, SAMPLE_IMSI);
        rawTablesPopulator.createAndPopulateRawSgehErrTable(valuesForSGEHTable, timestamp, connection);
        final Map<String, Object> valuesForLTETable = new HashMap<String, Object>();
        valuesForLTETable.put(EVENT_ID, HANDOVER_IN_4G);
        valuesForLTETable.put(RAT, RAT_FOR_LTE);
        valuesForLTETable.put(IMSI, SAMPLE_IMSI);
        rawTablesPopulator.createAndPopulateRawLteErrTable(valuesForLTETable, timestamp, connection);
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

    private void insertRowIntoCCTable(final String table, final int causeCode, final int causeProtoType,
            final String ccDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_CODE_COLUMN, causeCode);
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_CODE_DESC_COLUMN, ccDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoSCCTable(final String table, final int subCauseCode, final String sccDesc)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(SUBCAUSE_CODE_COLUMN, subCauseCode);
        valuesForTable.put(SUB_CAUSE_CODE_DESC_COLUMN, sccDesc);
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
        insertRowIntoCCTable(TEMP_DIM_E_SGEH_CAUSECODE, 0, 0, "");
        insertRowIntoCCTable(TEMP_DIM_E_LTE_CAUSECODE, 0, 0, "");
    }

    private void populateSCCTables() throws SQLException {
        insertRowIntoSCCTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, 0, "");
        insertRowIntoSCCTable(TEMP_DIM_E_LTE_SUBCAUSECODE, 0, "");

    }

    private void populateCPTTables() throws SQLException {
        insertRowIntoCPTTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, 0, "");

        insertRowIntoCPTTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, 0, "");
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
