/**
 * 
 */
package com.ericsson.eniq.events.server.integritytests.eventanalysis.apn;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ebhasou
 * 
 */
public class EventAnalysisSummaryWithPreparedTablesApnTest extends BaseDataIntegrityTest<EventAnalysisDetailedResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private final static Map<String, Object> columnsInLteRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSgehRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInDTPDPRawTables = new HashMap<String, Object>();

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);

        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        setupRawTableValues(timestamp, SAMPLE_APN, ATTACH_IN_2G_AND_3G, RAT_FOR_GSM, 0);
        final Collection<String> columnsToCreateInSgehTable = new ArrayList<String>();
        columnsToCreateInSgehTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInSgehTable.addAll(columnsInSgehRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsToCreateInSgehTable);
        final Map<String, Object> valuesForSgehTable = new HashMap<String, Object>();
        valuesForSgehTable.putAll(columnsInRawTables);
        valuesForSgehTable.putAll(columnsInSgehRawTables);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, valuesForSgehTable);

        setupRawTableValues(timestamp, SAMPLE_APN, ATTACH_IN_4G, RAT_FOR_LTE, 0);
        final Collection<String> columnsToCreateInLteTable = new ArrayList<String>();
        columnsToCreateInLteTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInLteTable.addAll(columnsInLteRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsToCreateInLteTable);
        final Map<String, Object> valuesForLteTable = new HashMap<String, Object>();
        valuesForLteTable.putAll(columnsInRawTables);
        valuesForLteTable.putAll(columnsInLteRawTables);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForLteTable);
        setupRawTableValues(timestamp, SAMPLE_APN, ATTACH_IN_4G, RAT_FOR_LTE, 1);
        final Map<String, Object> valuesForSecondRowInLteTable = new HashMap<String, Object>();
        valuesForSecondRowInLteTable.putAll(columnsInRawTables);
        valuesForSecondRowInLteTable.putAll(columnsInLteRawTables);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForSecondRowInLteTable);

        setupRawTableValues(timestamp, SAMPLE_APN, ATTACH_IN_4G, RAT_FOR_LTE, 0);
        final Collection<String> columnsToCreateInDTPDPTable = new ArrayList<String>();
        columnsToCreateInDTPDPTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInDTPDPTable.addAll(columnsInLteRawTables.keySet());
        columnsToCreateInDTPDPTable.addAll(columnsInDTPDPRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_RAW, columnsToCreateInDTPDPTable);
        final Map<String, Object> valuesForDTPDPTable = new HashMap<String, Object>();
        valuesForDTPDPTable.putAll(columnsInRawTables);
        valuesForDTPDPTable.putAll(columnsInLteRawTables);
        valuesForDTPDPTable.putAll(columnsInDTPDPRawTables);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, valuesForDTPDPTable);
        createTemporaryTables();
    }

    @Test
    public void testGetDetailedData_APN_1Week() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        DummyUriInfoImpl.setUriInfo(map, eventAnalysisService);

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(3));

    }

    @Test
    public void testGetDetailedData_APN_1Week_FilterOnQCI1() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(QCI_ID, ID_FOR_QCI_1);

        DummyUriInfoImpl.setUriInfo(map, eventAnalysisService);

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));

    }

    private void setupRawTableValues(final String timestamp, final String apn, final int eventId, final int rat,
            final int valueForQCIErr1) {
        columnsInRawTables.put(EVENT_ID, eventId);
        columnsInRawTables.put(RAT, rat);
        columnsInRawTables.put(TAC, 124);
        columnsInRawTables.put(IMSI, 123);
        columnsInRawTables.put(DATETIME_ID, timestamp);
        columnsInRawTables.put(APN, apn);
        columnsInRawTables.put(EVENT_TIME, timestamp);
        columnsInRawTables.put(EVENT_SOURCE_NAME, "");
        columnsInRawTables.put(HIERARCHY_1, "");
        columnsInRawTables.put(HIERARCHY_2, "");
        columnsInRawTables.put(HIERARCHY_3, "");
        columnsInRawTables.put(VENDOR_PARAM_UPPER_CASE, "");
        columnsInRawTables.put(DURATION, 0);
        columnsInRawTables.put(CAUSE_PROT_TYPE_COLUMN, 0);
        columnsInRawTables.put(CAUSE_CODE_COLUMN, 0);
        columnsInRawTables.put(SUBCAUSE_CODE_COLUMN, 0);
        columnsInRawTables.put(EVENT_RESULT, 0);
        columnsInRawTables.put(ATTACH_TYPE, 0);
        columnsInRawTables.put(DETACH_TYPE, 0);
        columnsInRawTables.put(DEACTIVATION_TRIGGER, 0);
        columnsInRawTables.put(DETACH_TRIGGER, 0);
        columnsInRawTables.put(SERVICE_REQ_TRIGGER, 0);
        columnsInRawTables.put(PAGING_ATTEMPTS, 0);
        columnsInRawTables.put(REQUEST_RETRIES, 0);
        columnsInRawTables.put(MCC, "0");
        columnsInRawTables.put(MNC, "0");
        columnsInRawTables.put(OLD_MCC, "0");
        columnsInRawTables.put(OLD_MNC, "0");
        columnsInRawTables.put(OLD_RAC, 0);
        columnsInRawTables.put(OLD_LAC, 0);
        columnsInRawTables.put(TRANSFERRED_PDP, 0);
        columnsInRawTables.put(HLR, 0);
        columnsInRawTables.put(DROPPED_PDP, 0);
        columnsInRawTables.put(MSISDN, 0);

        columnsInSgehRawTables.put(PTMSI, 3);
        columnsInSgehRawTables.put(LINKED_NSAPI, 0);
        columnsInSgehRawTables.put(PDP_NSAPI_1, 0);
        columnsInSgehRawTables.put(PDP_NSAPI_2, 0);
        columnsInSgehRawTables.put(PDP_GGSN_IPADDRESS_1, 0);
        columnsInSgehRawTables.put(PDP_GGSN_IPADDRESS_2, 0);
        columnsInSgehRawTables.put(PDP_MS_IPADDRESS_1, 0);
        columnsInSgehRawTables.put(PDP_MS_IPADDRESS_2, 0);
        columnsInSgehRawTables.put(PDP_GGSN_NAME_1, "0");
        columnsInSgehRawTables.put(PDP_GGSN_NAME_2, "0");
        columnsInSgehRawTables.put(RAC, 0);
        columnsInSgehRawTables.put(LAC, 0);
        columnsInSgehRawTables.put(UPDATE_TYPE, 0);
        columnsInSgehRawTables.put(OLD_SGSN_IPADDRESS, 0);

        columnsInLteRawTables.put(TRAC, 0);
        columnsInLteRawTables.put(OLD_TRAC, 0);
        columnsInLteRawTables.put(OLD_CELL_ID, 0);
        columnsInLteRawTables.put(OLD_L_MMEGI, 0);
        columnsInLteRawTables.put(OLD_L_MMEC, 0);
        columnsInLteRawTables.put(OLD_L_MTMSI, 0);
        columnsInLteRawTables.put(OLD_SGW_IPV4, 0);
        columnsInLteRawTables.put(OLD_SGW_IPV6, 0);
        columnsInLteRawTables.put(PDN_BEARER_ID_1, 0);
        columnsInLteRawTables.put(PDN_BEARER_ID_2, 0);
        columnsInLteRawTables.put(PDN_BEARER_ID_3, 0);
        columnsInLteRawTables.put(PDN_PAA_IPV4_1, 0);
        columnsInLteRawTables.put(PDN_PAA_IPV4_2, 0);
        columnsInLteRawTables.put(PDN_PAA_IPV4_3, 0);
        columnsInLteRawTables.put(PDN_PAA_IPV6_1, 0);
        columnsInLteRawTables.put(PDN_PAA_IPV6_2, 0);
        columnsInLteRawTables.put(PDN_PAA_IPV6_3, 0);
        columnsInLteRawTables.put(PDN_PGW_IPV4_1, 0);
        columnsInLteRawTables.put(PDN_PGW_IPV4_2, 0);
        columnsInLteRawTables.put(PDN_PGW_IPV4_3, 0);
        columnsInLteRawTables.put(PDN_PGW_IPV6_1, 0);
        columnsInLteRawTables.put(PDN_PGW_IPV6_2, 0);
        columnsInLteRawTables.put(PDN_PGW_IPV6_3, 0);
        columnsInLteRawTables.put(ARP_PL_1, 0);
        columnsInLteRawTables.put(ARP_PL_2, 0);
        columnsInLteRawTables.put(ARP_PL_3, 0);
        columnsInLteRawTables.put(GBR_UPLINK_1, 0);
        columnsInLteRawTables.put(GBR_UPLINK_2, 0);
        columnsInLteRawTables.put(GBR_UPLINK_3, 0);
        columnsInLteRawTables.put(GBR_DOWNLINK_1, 0);
        columnsInLteRawTables.put(GBR_DOWNLINK_2, 0);
        columnsInLteRawTables.put(GBR_DOWNLINK_3, 0);
        columnsInLteRawTables.put(L_DISCONNECT_PDN_TYPE, 0);
        columnsInLteRawTables.put(EVENT_SUBTYPE_ID, 0);
        columnsInLteRawTables.put(SMS_ONLY, 0);
        columnsInLteRawTables.put(COMBINED_TAU_TYPE, 0);
        columnsInLteRawTables.put(ARP_PCI_1, 0);
        columnsInLteRawTables.put(ARP_PCI_2, 0);
        columnsInLteRawTables.put(ARP_PCI_3, 0);
        columnsInLteRawTables.put(ARP_PVI_1, 0);
        columnsInLteRawTables.put(ARP_PVI_2, 0);
        columnsInLteRawTables.put(ARP_PVI_3, 0);
        columnsInLteRawTables.put(BEARER_CAUSE_1, 0);
        columnsInLteRawTables.put(BEARER_CAUSE_2, 0);
        columnsInLteRawTables.put(BEARER_CAUSE_3, 0);
        columnsInLteRawTables.put(QCI_ERR_1, valueForQCIErr1);
        columnsInLteRawTables.put(QCI_ERR_2, 0);
        columnsInLteRawTables.put(QCI_ERR_3, 0);
        columnsInLteRawTables.put(QCI_ERR_4, 0);
        columnsInLteRawTables.put(QCI_ERR_5, 0);
        columnsInLteRawTables.put(QCI_ERR_6, 0);
        columnsInLteRawTables.put(QCI_ERR_7, 0);
        columnsInLteRawTables.put(QCI_ERR_8, 0);
        columnsInLteRawTables.put(QCI_ERR_9, 0);
        columnsInLteRawTables.put(QCI_ERR_10, 0);
        columnsInLteRawTables.put(QCI_SUC_1, 0);
        columnsInLteRawTables.put(QCI_SUC_2, 0);
        columnsInLteRawTables.put(QCI_SUC_3, 0);
        columnsInLteRawTables.put(QCI_SUC_4, 0);
        columnsInLteRawTables.put(QCI_SUC_5, 0);
        columnsInLteRawTables.put(QCI_SUC_6, 0);
        columnsInLteRawTables.put(QCI_SUC_7, 0);
        columnsInLteRawTables.put(QCI_SUC_8, 0);
        columnsInLteRawTables.put(QCI_SUC_9, 0);
        columnsInLteRawTables.put(QCI_SUC_10, 0);
        columnsInLteRawTables.put(EPS_BEARER_ID_1, 0);
        columnsInLteRawTables.put(EPS_BEARER_ID_2, 0);
        columnsInLteRawTables.put(EPS_BEARER_ID_3, 0);

        columnsInDTPDPRawTables.put(APN, apn);
        columnsInDTPDPRawTables.put(TAC, 124);
        columnsInDTPDPRawTables.put(QOS_MEAN_DATAVOL, 0);
        columnsInDTPDPRawTables.put(DATAVOL_UL, 0);
        columnsInDTPDPRawTables.put(DATAVOL_DL, 0);
        columnsInDTPDPRawTables.put(PEAK_USAGE_UL, 0);
        columnsInDTPDPRawTables.put(PEAK_USAGE_DL, 0);
        columnsInDTPDPRawTables.put(QOS_HOP_UP, 0);
        columnsInDTPDPRawTables.put(QOS_HOP_DOWN, 0);
        columnsInDTPDPRawTables.put(SGSN_NAME_1, "");
        columnsInDTPDPRawTables.put(SGSN_NAME_2, "");
        columnsInDTPDPRawTables.put(SGSN_NAME_3, "");
        columnsInDTPDPRawTables.put(SGSN_NAME_4, "");
        columnsInDTPDPRawTables.put(SGSN_NAME_5, "");
        columnsInDTPDPRawTables.put(DATETIME_ID, timestamp);
        columnsInDTPDPRawTables.put(IMSI, 123);
        columnsInDTPDPRawTables.put(CHARGING_ID, 0);
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
