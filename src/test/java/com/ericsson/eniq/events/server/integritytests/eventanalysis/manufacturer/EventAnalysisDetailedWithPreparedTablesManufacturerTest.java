/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventanalysis.manufacturer;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * This is to verify that on entering dummy manufacturer TAC to DB we my get the query results for the manufacturer. 
 * 
 * @author eflatib
 * @since 2011
 *
 */
public class EventAnalysisDetailedWithPreparedTablesManufacturerTest extends
        BaseDataIntegrityTest<EventAnalysisDetailedResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private final static Map<String, Object> columnsInLteRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSgehRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInDTPDPRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInCauseCodeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSubCauseCodeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInLteCauseProtTypeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSgehCauseProtTypeTable = new HashMap<String, Object>();

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);

        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        setupRawTableValues(timestamp, SONY_ERICSSON_TAC, ATTACH_IN_2G_AND_3G, RAT_FOR_GSM, 0);
        final Collection<String> columnsToCreateInSgehTable = new ArrayList<String>();
        columnsToCreateInSgehTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInSgehTable.addAll(columnsInSgehRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsToCreateInSgehTable);
        final Map<String, Object> valuesForSgehTable = new HashMap<String, Object>();
        valuesForSgehTable.putAll(columnsInRawTables);
        valuesForSgehTable.putAll(columnsInSgehRawTables);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, valuesForSgehTable);

        setupRawTableValues(timestamp, SONY_ERICSSON_TAC, ATTACH_IN_4G, RAT_FOR_LTE, 0);
        final Collection<String> columnsToCreateInLteTable = new ArrayList<String>();
        columnsToCreateInLteTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInLteTable.addAll(columnsInLteRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsToCreateInLteTable);
        final Map<String, Object> valuesForLteTable = new HashMap<String, Object>();
        valuesForLteTable.putAll(columnsInRawTables);
        valuesForLteTable.putAll(columnsInLteRawTables);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForLteTable);

        setupRawTableValues(timestamp, SONY_ERICSSON_TAC, ATTACH_IN_4G, RAT_FOR_LTE, 1);
        final Map<String, Object> valuesForSecondRowInLteTable = new HashMap<String, Object>();
        valuesForSecondRowInLteTable.putAll(columnsInRawTables);
        valuesForSecondRowInLteTable.putAll(columnsInLteRawTables);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForSecondRowInLteTable);

        setupRawTableValues(timestamp, SONY_ERICSSON_TAC, ATTACH_IN_4G, RAT_FOR_LTE, 0);
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

        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSECODE, columnsInCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_CAUSECODE, columnsInCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsInSubCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsInSubCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_LTE_CAUSECODE, columnsInCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_LTE_CAUSECODE, columnsInCauseCodeTable);
        createTemporaryTable(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsInSubCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsInSubCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsInLteCauseProtTypeTable.keySet());
        insertRow(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsInLteCauseProtTypeTable);

        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsInSgehCauseProtTypeTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsInSgehCauseProtTypeTable);

    }

    @Test
    public void testGetDetailedData_Manufacturer_1Week() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);

        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(MAN_PARAM, SONY_ERICSSON);

        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 3!", detailedResult.size(), is(3));

    }

    @Test
    public void testGetDetailedData_APN_1Week_FilterOnQCI1() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(MAN_PARAM, SONY_ERICSSON);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(QCI_ID, ID_FOR_QCI_1);

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));
        //System.out.println("Result size: " + detailedResult.size());

    }

    private void setupRawTableValues(final String timestamp, final int tac, final int eventId, final int rat,
            final int valueForQCIErr1) {

        columnsInSgehCauseProtTypeTable.put(CAUSE_PROT_TYPE_COLUMN, "0");
        columnsInSgehCauseProtTypeTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, "RIL3");

        columnsInLteCauseProtTypeTable.put(CAUSE_PROT_TYPE_COLUMN, "0");
        columnsInLteCauseProtTypeTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, "S1AP");

        columnsInSubCauseCodeTable.put(SUBCAUSE_CODE_COLUMN, "0");
        columnsInSubCauseCodeTable.put(SUB_CAUSE_CODE_DESC_COLUMN, "No SubCauseCode available");
        columnsInSubCauseCodeTable.put(SUB_CAUSE_CODE_HELP_COLUMN, "");

        columnsInCauseCodeTable.put(CAUSE_CODE_COLUMN, "0");
        columnsInCauseCodeTable.put(CAUSE_CODE_DESC_COLUMN, "Success");
        columnsInCauseCodeTable.put(CAUSE_CODE_HELP_COLUMN, "");
        columnsInCauseCodeTable.put(CAUSE_PROT_TYPE_COLUMN, "0");

        columnsInRawTables.put(EVENT_ID, eventId);
        columnsInRawTables.put(RAT, rat);
        columnsInRawTables.put(TAC, tac); // Manufacturer TAC
        columnsInRawTables.put(IMSI, 123);
        columnsInRawTables.put(DATETIME_ID, timestamp);
        columnsInRawTables.put(APN, SAMPLE_APN);
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

        columnsInDTPDPRawTables.put(APN, SAMPLE_APN);
        columnsInDTPDPRawTables.put(TAC, tac);
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
}
