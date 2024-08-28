/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.roaminganalysis;


import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.test.queryresults.network.RoamingDrillDetailResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 *
 * @author ezhelao
 * @since 01/2012
 */
public class RoamingDrillByCountryDetailRawTest extends
        BaseDataIntegrityTest<RoamingDrillDetailResult> {


    private static final String ROAMING = "ROAMING";
    private static final String IMSI = "IMSI";
    private static final String IMSI_MCC = "IMSI_MCC";
    private static final String IMSI_MNC = "IMSI_MNC";
    private static final String TEMP_EVENT_E_SGEH_ERR_RAW = "#EVENT_E_SGEH_ERR_RAW";
    private static final String TEMP_EVENT_E_LTE_ERR_RAW = "#EVENT_E_LTE_ERR_RAW";
    private static final String TAC = "TAC";
    private static final String NORWAY_MCC = "242";
    private static final String TELENOR_MNC = "01";
    private static final String EVENT_TIME = "EVENT_TIME";
    private static final String PTMSI = "PTMSI";
    private static final String EVENT_SOURCE_NAME = "EVENT_SOURCE_NAME";


    RoamingDrillByCountryDetailService roamingDrillByCountryDetailService;


    @Before
    public void setup() throws Exception {
        roamingDrillByCountryDetailService = new RoamingDrillByCountryDetailService();
        attachDependencies(roamingDrillByCountryDetailService);
        createEventRawErrTable();

        seedTacTable();
        insertAllRawData();

        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_MCCMNC);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_EVENTTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_ATTACHTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_CAUSECODE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_DEACTIVATIONTRIGGER);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_DETACH_TRIGGER);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_DETACH_TYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_EVENTRESULT);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_GGSN);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_RAT);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_TAC);

        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_ARP_PCI);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_ARP_PVI);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_ATTACHTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_BEARER_CAUSE);

        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_CAUSE_PROT_TYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_CAUSECODE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_COMBINED_TAU_TYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_CAUSECODE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_COMBINED_TAU_TYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_DEACTIVATIONTRIGGER);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_DETACH_TRIGGER);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_DETACH_TYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_EVENT_SUBTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_EVENTTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_PDNDISCONNECTTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_SERVICE_REQ_TRIGGER);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_SMS_ONLY);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_SUBCAUSECODE);


        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_SERVICE_REQ_TRIGGER);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_SGSN);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_SUBCAUSECODE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_CAUSE_PROT_TYPE);


        createAndPopulateTempLookupTable(DIM_E_LTE_ARP_PCI);
        createAndPopulateTempLookupTable(DIM_E_LTE_ARP_PVI);
        createAndPopulateTempLookupTable(DIM_E_LTE_ATTACHTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_BEARER_CAUSE);
        createAndPopulateTempLookupTable(DIM_E_LTE_CAUSE_PROT_TYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_CAUSECODE);
        createAndPopulateTempLookupTable(DIM_E_LTE_COMBINED_TAU_TYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_DEACTIVATIONTRIGGER);
        createAndPopulateTempLookupTable(DIM_E_LTE_DETACH_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_LTE_DETACH_TYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_EVENT_SUBTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_EVENTTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_PDNDISCONNECTTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_SERVICE_REQ_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_LTE_SMS_ONLY);
        createAndPopulateTempLookupTable(DIM_E_LTE_SUBCAUSECODE);


        createAndPopulateTempLookupTable(DIM_E_SGEH_MCCMNC);
        createAndPopulateTempLookupTable(DIM_E_SGEH_EVENTTYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_ATTACHTYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_CAUSECODE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_DEACTIVATIONTRIGGER);

        createAndPopulateTempLookupTable(DIM_E_SGEH_DETACH_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_SGEH_DETACH_TYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_EVENTRESULT);
        createAndPopulateTempLookupTable(DIM_E_SGEH_GGSN);
        createAndPopulateTempLookupTable(DIM_E_SGEH_RAT);
        createAndPopulateTempLookupTable(DIM_E_SGEH_SERVICE_REQ_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_SGEH_SGSN);
        createAndPopulateTempLookupTable(DIM_E_SGEH_SUBCAUSECODE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_CAUSE_PROT_TYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_TAC);



    }

    @Test
    public void testFiveMinuteQuery_SgehRaw() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, "30");
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.add(MCC_PARAM,NORWAY_MCC);
        requestParameters.add(EVENT_ID_PARAM,"1");
        requestParameters.add(MAX_ROWS, DEFAULT_MAX_ROWS);
        final List<RoamingDrillDetailResult> results = runQueryAssertJSONStringTransform(roamingDrillByCountryDetailService, requestParameters);
        assertThat(results.size(),is(4));

    }

    @Test
    public void testFiveMinuteQuery_LteRaw() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, "30");
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.add(MCC_PARAM,NORWAY_MCC);
        requestParameters.add(EVENT_ID_PARAM,"7");
        requestParameters.add(MAX_ROWS, DEFAULT_MAX_ROWS);
        final List<RoamingDrillDetailResult> results = runQueryAssertJSONStringTransform(roamingDrillByCountryDetailService, requestParameters);
        assertThat(results.size(),is(5));

    }


    @Test
    public void testMaxRows() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, "30");
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.add(MCC_PARAM, NORWAY_MCC);
        requestParameters.add(EVENT_ID_PARAM, "7");
        requestParameters.add(MAX_ROWS, "3");
        List<RoamingDrillDetailResult> results = runQueryAssertJSONStringTransform(roamingDrillByCountryDetailService, requestParameters);
        assertThat(results.size(), is(3));

    }


    private void insertAllRawData() throws SQLException {
        String dateTime = DateTimeUtilities.getDateTimeMinus25Minutes();
        insertRawSgehDataRow(dateTime, dateTime, NORWAY_MCC, TELENOR_MNC, "1");
        insertRawSgehDataRow(dateTime, dateTime, NORWAY_MCC, TELENOR_MNC, "2");
        insertRawSgehDataRow(dateTime, dateTime, NORWAY_MCC, TELENOR_MNC, "3");
        insertRawSgehDataRow(dateTime, dateTime, NORWAY_MCC, TELENOR_MNC, "4");
        insertRawSgehDataRow(dateTime, dateTime, "100", TELENOR_MNC, "5");


        insertRawLteDataRow(dateTime,dateTime,NORWAY_MCC,TELENOR_MNC,"11");
        insertRawLteDataRow(dateTime,dateTime,NORWAY_MCC,TELENOR_MNC,"12");
        insertRawLteDataRow(dateTime,dateTime,NORWAY_MCC,TELENOR_MNC,"13");
        insertRawLteDataRow(dateTime,dateTime,NORWAY_MCC,TELENOR_MNC,"14");
        insertRawLteDataRow(dateTime,dateTime,NORWAY_MCC,TELENOR_MNC,"15");



    }



    private void insertRawLteDataRow (String datatime_id, String event_time, String imsi_mcc, String imsi_mnc, String imsi) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();

        valuesForTable.put(BEARER_CAUSE_3,0);
        valuesForTable.put(BEARER_CAUSE_2,0);
        valuesForTable.put(BEARER_CAUSE_1,0);


        valuesForTable.put(ARP_PVI_3,0);
        valuesForTable.put(ARP_PVI_2,0);
        valuesForTable.put(ARP_PVI_1,0);
        
        valuesForTable.put(ARP_PCI_3,0);
        valuesForTable.put(ARP_PCI_2,0);
        valuesForTable.put(ARP_PCI_1,0);
        valuesForTable.put(COMBINED_TAU_TYPE,0);
        valuesForTable.put(SMS_ONLY,0);
        valuesForTable.put(EVENT_SUBTYPE_ID,0);
        valuesForTable.put(L_DISCONNECT_PDN_TYPE,0);
        valuesForTable.put(MCC,"242");
        valuesForTable.put(MNC,"01");
        valuesForTable.put(QCI_SUC_10,0);
        valuesForTable.put(QCI_SUC_9,0);
        valuesForTable.put(QCI_SUC_8,0);
        valuesForTable.put(QCI_SUC_7,0);
        valuesForTable.put(QCI_SUC_6,0);
        valuesForTable.put(QCI_SUC_5,0);
        valuesForTable.put(QCI_SUC_4,0);
        valuesForTable.put(QCI_SUC_3,0);
        valuesForTable.put(QCI_SUC_2,0);
        valuesForTable.put(QCI_SUC_1,0);


        valuesForTable.put(QCI_ERR_10,0);
        valuesForTable.put(QCI_ERR_9,0);
        valuesForTable.put(QCI_ERR_8,0);
        valuesForTable.put(QCI_ERR_7,0);
        valuesForTable.put(QCI_ERR_6,0);
        valuesForTable.put(QCI_ERR_5,0);
        valuesForTable.put(QCI_ERR_4,0);
        valuesForTable.put(QCI_ERR_3,0);
        valuesForTable.put(QCI_ERR_2,0);
        valuesForTable.put(QCI_ERR_1,0);
        valuesForTable.put(EPS_BEARER_ID_3,0);
        valuesForTable.put(EPS_BEARER_ID_2,0);
        valuesForTable.put(EPS_BEARER_ID_1,0);
        valuesForTable.put(GBR_DOWNLINK_3,0);
        valuesForTable.put(GBR_DOWNLINK_2,0);
        valuesForTable.put(GBR_DOWNLINK_1,0);
        valuesForTable.put(GBR_UPLINK_3,0);
        valuesForTable.put(GBR_UPLINK_2,0);
        valuesForTable.put(GBR_UPLINK_1,0);
        valuesForTable.put(ARP_PL_3,0);
        valuesForTable.put(ARP_PL_2,0);
        valuesForTable.put(ARP_PL_1,0);
        valuesForTable.put(PDN_PGW_IPV6_3,0);
        valuesForTable.put(PDN_PGW_IPV6_2,0);
        valuesForTable.put(PDN_PGW_IPV6_1,0);
        valuesForTable.put(PDN_PGW_IPV4_3,0);
        valuesForTable.put(PDN_PGW_IPV4_2,0);
        valuesForTable.put(PDN_PGW_IPV4_1,0);
        valuesForTable.put(PDN_PAA_IPV6_3,0);
        valuesForTable.put(PDN_PAA_IPV6_2,0);
        valuesForTable.put(PDN_PAA_IPV6_1,0);
        valuesForTable.put(PDN_PAA_IPV4_3,0);
        valuesForTable.put(PDN_PAA_IPV4_2,0);
        valuesForTable.put(PDN_PAA_IPV4_1,0);
        valuesForTable.put(PDN_BEARER_ID_3,0);
        valuesForTable.put(PDN_BEARER_ID_2,0);
        valuesForTable.put(PDN_BEARER_ID_1,0);
        valuesForTable.put(OLD_SGW_IPV6,0);
        valuesForTable.put(OLD_SGW_IPV4,0);
        valuesForTable.put(OLD_L_MTMSI,0);
        valuesForTable.put(OLD_L_MMEC,0);
        valuesForTable.put(OLD_L_MMEGI,0);
        valuesForTable.put(OLD_CELL_ID,0);
        valuesForTable.put(OLD_TRAC,0);
        valuesForTable.put(TRAC,0);
        valuesForTable.put(MSISDN,0);
        valuesForTable.put(RAT,0);
        valuesForTable.put(SUBCAUSE_CODE,0);
        valuesForTable.put(SERVICE_REQ_TRIGGER,0);
        valuesForTable.put(DETACH_TYPE,0);
        valuesForTable.put(DETACH_TRIGGER,0);
        valuesForTable.put(DEACTIVATION_TRIGGER,0);
        valuesForTable.put(CAUSE_CODE_COL,0);
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN,0);
        valuesForTable.put(ATTACH_TYPE,0);
        valuesForTable.put(EVENT_RESULT,0);
        valuesForTable.put(ROAMING,1);
        valuesForTable.put(EVENT_ID,"7");
        valuesForTable.put(DATETIME_ID,datatime_id);
        valuesForTable.put(EVENT_TIME,event_time);
        valuesForTable.put(IMSI,imsi);
        valuesForTable.put(TAC,"1280600");
        valuesForTable.put(EVENT_SOURCE_NAME,randomString());
        valuesForTable.put(HIERARCHY_3,randomString());
        valuesForTable.put(HIERARCHY_2,randomString());
        valuesForTable.put(HIERARCHY_1,randomString());
        valuesForTable.put(VENDOR,randomString());
        valuesForTable.put(PAGING_ATTEMPTS,randomUsignedInt());
        valuesForTable.put(REQUEST_RETRIES,randomUsignedInt());
        valuesForTable.put(APN,randomString());
        valuesForTable.put(IMSI_MCC,imsi_mcc);
        valuesForTable.put(IMSI_MNC,imsi_mnc);
        valuesForTable.put(OLD_MCC,randomString());
        valuesForTable.put(OLD_MNC,randomString());
        valuesForTable.put(DURATION,randomUsignedInt());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForTable);

        
        return ;
    }

    private void insertRawSgehDataRow(String datatime_id, String event_time, String imsi_mcc, String imsi_mnc, String imsi) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        
        valuesForTable.put(BEARER_CAUSE_3,0);
        valuesForTable.put(BEARER_CAUSE_2,0);
        valuesForTable.put(BEARER_CAUSE_1,0);
        
        
        valuesForTable.put(ARP_PVI_3,0);
        valuesForTable.put(ARP_PVI_2,0);
        valuesForTable.put(ARP_PVI_1,0);
        

        valuesForTable.put(ARP_PCI_3,0);
        valuesForTable.put(ARP_PCI_2,0);
        valuesForTable.put(ARP_PCI_1,0);
        valuesForTable.put(COMBINED_TAU_TYPE,0);

        valuesForTable.put(SMS_ONLY,0);

        valuesForTable.put(EVENT_SUBTYPE_ID,0);

        valuesForTable.put(L_DISCONNECT_PDN_TYPE,0);

        valuesForTable.put(MCC,"12");
        valuesForTable.put(MNC,"12");
        
        valuesForTable.put(QCI_SUC_10,0);
        valuesForTable.put(QCI_SUC_9,0);
        valuesForTable.put(QCI_SUC_8,0);
        valuesForTable.put(QCI_SUC_7,0);
        valuesForTable.put(QCI_SUC_6,0);
        valuesForTable.put(QCI_SUC_5,0);
        valuesForTable.put(QCI_SUC_4,0);
        valuesForTable.put(QCI_SUC_3,0);
        valuesForTable.put(QCI_SUC_2,0);
        valuesForTable.put(QCI_SUC_1,0);
        
        
        valuesForTable.put(QCI_ERR_10,0);
        valuesForTable.put(QCI_ERR_9,0);
        valuesForTable.put(QCI_ERR_8,0);
        valuesForTable.put(QCI_ERR_7,0);
        valuesForTable.put(QCI_ERR_6,0);
        valuesForTable.put(QCI_ERR_5,0);
        valuesForTable.put(QCI_ERR_4,0);
        valuesForTable.put(QCI_ERR_3,0);
        valuesForTable.put(QCI_ERR_2,0);
        valuesForTable.put(QCI_ERR_1,0);
        valuesForTable.put(EPS_BEARER_ID_3,0);
        valuesForTable.put(EPS_BEARER_ID_2,0);
        valuesForTable.put(EPS_BEARER_ID_1,0);
        valuesForTable.put(GBR_DOWNLINK_3,0);
        valuesForTable.put(GBR_DOWNLINK_2,0);
        valuesForTable.put(GBR_DOWNLINK_1,0);
        valuesForTable.put(GBR_UPLINK_3,0);
        valuesForTable.put(GBR_UPLINK_2,0);
        valuesForTable.put(GBR_UPLINK_1,0);
        valuesForTable.put(ARP_PL_3,0);
        valuesForTable.put(ARP_PL_2,0);
        valuesForTable.put(ARP_PL_1,0);
        valuesForTable.put(PDN_PGW_IPV6_3,0);
        valuesForTable.put(PDN_PGW_IPV6_2,0);
        valuesForTable.put(PDN_PGW_IPV6_1,0);
        valuesForTable.put(PDN_PGW_IPV4_3,0);
        valuesForTable.put(PDN_PGW_IPV4_2,0);
        valuesForTable.put(PDN_PGW_IPV4_1,0);
        valuesForTable.put(PDN_PAA_IPV6_3,0);
        valuesForTable.put(PDN_PAA_IPV6_2,0);
        valuesForTable.put(PDN_PAA_IPV6_1,0);
        valuesForTable.put(PDN_PAA_IPV4_3,0);
        valuesForTable.put(PDN_PAA_IPV4_2,0);
        valuesForTable.put(PDN_PAA_IPV4_1,0);
        valuesForTable.put(PDN_BEARER_ID_3,0);
        valuesForTable.put(PDN_BEARER_ID_2,0);
        valuesForTable.put(PDN_BEARER_ID_1,0);
        valuesForTable.put(OLD_SGW_IPV6,0);
        valuesForTable.put(OLD_SGW_IPV4,0);
        valuesForTable.put(OLD_L_MTMSI,0);
        valuesForTable.put(OLD_L_MMEC,0);
        valuesForTable.put(OLD_L_MMEGI,0);
        valuesForTable.put(OLD_CELL_ID,0);
        valuesForTable.put(OLD_TRAC,0);
        valuesForTable.put(TRAC,0);
        valuesForTable.put(MSISDN,0);
        valuesForTable.put(OLD_SGSN_IPADDRESS, 0);
        valuesForTable.put(PDP_GGSN_IPADDRESS_2, 0);
        valuesForTable.put(PDP_GGSN_IPADDRESS_1, 0);
        valuesForTable.put(PDP_GGSN_NAME_1, randomString());
        valuesForTable.put(RAT, 0);
        valuesForTable.put(SUBCAUSE_CODE, 0);
        valuesForTable.put(SGSN_NAME, "SGSN1");
        valuesForTable.put(SERVICE_REQ_TRIGGER, 0);
        valuesForTable.put(GGSN_NAME, "GGSN1");
        valuesForTable.put(DETACH_TYPE, 0);
        valuesForTable.put(DETACH_TRIGGER, 0);
        valuesForTable.put(DEACTIVATION_TRIGGER, 0);
        valuesForTable.put(CAUSE_CODE_COL, 0);
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, 0);
        valuesForTable.put(ATTACH_TYPE, 0);
        valuesForTable.put(EVENT_RESULT, 0);
        valuesForTable.put(ROAMING, 1);
        valuesForTable.put(EVENT_ID, "1");
        valuesForTable.put(DATETIME_ID, datatime_id);
        valuesForTable.put(EVENT_TIME, event_time);
        valuesForTable.put(IMSI, imsi);
        valuesForTable.put(PTMSI, randomUsignedInt());
        valuesForTable.put(TAC, "1280600");
        valuesForTable.put(EVENT_SOURCE_NAME, randomString());
        valuesForTable.put(HIERARCHY_3, randomString());
        valuesForTable.put(HIERARCHY_2, randomString());
        valuesForTable.put(HIERARCHY_1, randomString());
        valuesForTable.put(VENDOR, randomString());
        valuesForTable.put(LINKED_NSAPI, randomUsignedInt());
        valuesForTable.put(PDP_NSAPI_1, randomUsignedInt());
        valuesForTable.put(PDP_MS_IPADDRESS_1, randomUsignedInt());
        valuesForTable.put(PDP_NSAPI_2, randomUsignedInt());
        valuesForTable.put(PDP_GGSN_NAME_2, randomString());
        valuesForTable.put(PDP_MS_IPADDRESS_2, randomUsignedInt());
        valuesForTable.put(PAGING_ATTEMPTS, randomUsignedInt());
        valuesForTable.put(REQUEST_RETRIES, randomUsignedInt());
        valuesForTable.put(APN, randomString());
        valuesForTable.put(IMSI_MCC, imsi_mcc);
        valuesForTable.put(IMSI_MNC, imsi_mnc);
        valuesForTable.put(RAC, randomUsignedInt());
        valuesForTable.put(LAC, randomUsignedInt());
        valuesForTable.put(UPDATE_TYPE, randomUsignedInt());
        valuesForTable.put(OLD_SGSN_NAME, randomString());
        valuesForTable.put(OLD_MCC, randomString());
        valuesForTable.put(OLD_MNC, randomString());
        valuesForTable.put(OLD_RAC, randomUsignedInt());
        valuesForTable.put(OLD_LAC, randomUsignedInt());
        valuesForTable.put(TRANSFERRED_PDP, randomUsignedInt());
        valuesForTable.put(DROPPED_PDP, randomUsignedInt());
        valuesForTable.put(HLR, randomUsignedInt());
        valuesForTable.put(DURATION, randomUsignedInt());

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, valuesForTable);

    }

    private int randomUsignedInt() {
        return 0;

    }

    private String randomString()
    {
        return "0";
    }


    private  void createEventRawErrTable() throws Exception {
        createLteEventRawErrTable();
        createSgehEventRawErrTable();
    }

    private void createSgehEventRawErrTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(BEARER_CAUSE_3);
        columnsForEventTable.add(BEARER_CAUSE_2);
        columnsForEventTable.add(BEARER_CAUSE_1);
        
        columnsForEventTable.add(ARP_PVI_3);
        columnsForEventTable.add(ARP_PVI_2);
        columnsForEventTable.add(ARP_PVI_1);

        columnsForEventTable.add(ARP_PCI_3);
        columnsForEventTable.add(ARP_PCI_2);
        columnsForEventTable.add(ARP_PCI_1);
        columnsForEventTable.add(COMBINED_TAU_TYPE);
        columnsForEventTable.add(SMS_ONLY);
        columnsForEventTable.add(EVENT_SUBTYPE_ID);

        columnsForEventTable.add(L_DISCONNECT_PDN_TYPE);
        columnsForEventTable.add(MCC);
        columnsForEventTable.add(MNC);


        columnsForEventTable.add(QCI_SUC_10);
        columnsForEventTable.add(QCI_SUC_9);
        columnsForEventTable.add(QCI_SUC_8);
        columnsForEventTable.add(QCI_SUC_7);
        columnsForEventTable.add(QCI_SUC_6);
        columnsForEventTable.add(QCI_SUC_5);
        columnsForEventTable.add(QCI_SUC_4);
        columnsForEventTable.add(QCI_SUC_3);
        columnsForEventTable.add(QCI_SUC_2);
        columnsForEventTable.add(QCI_SUC_1);


        columnsForEventTable.add(QCI_ERR_10);
        columnsForEventTable.add(QCI_ERR_9);
        columnsForEventTable.add(QCI_ERR_8);
        columnsForEventTable.add(QCI_ERR_7);
        columnsForEventTable.add(QCI_ERR_6);
        columnsForEventTable.add(QCI_ERR_5);
        columnsForEventTable.add(QCI_ERR_4);
        columnsForEventTable.add(QCI_ERR_3);
        columnsForEventTable.add(QCI_ERR_2);
        columnsForEventTable.add(QCI_ERR_1);
        columnsForEventTable.add(EPS_BEARER_ID_3);
        columnsForEventTable.add(EPS_BEARER_ID_2);
        columnsForEventTable.add(EPS_BEARER_ID_1);
        columnsForEventTable.add(GBR_DOWNLINK_3);
        columnsForEventTable.add(GBR_DOWNLINK_2);
        columnsForEventTable.add(GBR_DOWNLINK_1);
        columnsForEventTable.add(GBR_UPLINK_3);
        columnsForEventTable.add(GBR_UPLINK_2);
        columnsForEventTable.add(GBR_UPLINK_1);
        columnsForEventTable.add(ARP_PL_3);
        columnsForEventTable.add(ARP_PL_2);
        columnsForEventTable.add(ARP_PL_1);
        columnsForEventTable.add(PDN_PGW_IPV6_3);
        columnsForEventTable.add(PDN_PGW_IPV6_2);
        columnsForEventTable.add(PDN_PGW_IPV6_1);
        columnsForEventTable.add(PDN_PGW_IPV4_3);
        columnsForEventTable.add(PDN_PGW_IPV4_2);
        columnsForEventTable.add(PDN_PGW_IPV4_1);
        columnsForEventTable.add(PDN_PAA_IPV6_3);
        columnsForEventTable.add(PDN_PAA_IPV6_2);
        columnsForEventTable.add(PDN_PAA_IPV6_1);
        columnsForEventTable.add(PDN_PAA_IPV4_3);
        columnsForEventTable.add(PDN_PAA_IPV4_2);
        columnsForEventTable.add(PDN_PAA_IPV4_1);
        columnsForEventTable.add(PDN_BEARER_ID_3);
        columnsForEventTable.add(PDN_BEARER_ID_2);
        columnsForEventTable.add(PDN_BEARER_ID_1);
        columnsForEventTable.add(OLD_SGW_IPV6);
        columnsForEventTable.add(OLD_SGW_IPV4);
        columnsForEventTable.add(OLD_L_MTMSI);
        columnsForEventTable.add(OLD_L_MMEC);
        columnsForEventTable.add(OLD_L_MMEGI);
        columnsForEventTable.add(OLD_CELL_ID);
        columnsForEventTable.add(OLD_TRAC);
        columnsForEventTable.add(TRAC);
        columnsForEventTable.add(MSISDN);
        columnsForEventTable.add(OLD_SGSN_IPADDRESS);
        columnsForEventTable.add(PDP_GGSN_IPADDRESS_2);
        columnsForEventTable.add(PDP_GGSN_IPADDRESS_1);
        columnsForEventTable.add(PDP_GGSN_NAME_1);
        columnsForEventTable.add(RAT);
        columnsForEventTable.add(SUBCAUSE_CODE);
        columnsForEventTable.add(SGSN_NAME);
        columnsForEventTable.add(SERVICE_REQ_TRIGGER);
        columnsForEventTable.add(GGSN_NAME);
        columnsForEventTable.add(DETACH_TYPE);
        columnsForEventTable.add(DETACH_TRIGGER);
        columnsForEventTable.add(DEACTIVATION_TRIGGER);
        columnsForEventTable.add(CAUSE_CODE_COL);
        columnsForEventTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForEventTable.add(ATTACH_TYPE);
        columnsForEventTable.add(EVENT_RESULT);
        columnsForEventTable.add(ROAMING);
        columnsForEventTable.add(EVENT_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(PTMSI);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(EVENT_SOURCE_NAME);
        columnsForEventTable.add(HIERARCHY_3);
        columnsForEventTable.add(HIERARCHY_2);
        columnsForEventTable.add(HIERARCHY_1);
        columnsForEventTable.add(VENDOR);
        columnsForEventTable.add(LINKED_NSAPI);
        columnsForEventTable.add(PDP_NSAPI_1);
        columnsForEventTable.add(PDP_MS_IPADDRESS_1);
        columnsForEventTable.add(PDP_NSAPI_2);
        columnsForEventTable.add(PDP_GGSN_NAME_2);
        columnsForEventTable.add(PDP_MS_IPADDRESS_2);
        columnsForEventTable.add(PAGING_ATTEMPTS);
        columnsForEventTable.add(REQUEST_RETRIES);
        columnsForEventTable.add(APN);
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(IMSI_MNC);
        columnsForEventTable.add(RAC);
        columnsForEventTable.add(LAC);
        columnsForEventTable.add(UPDATE_TYPE);
        columnsForEventTable.add(OLD_SGSN_NAME);
        columnsForEventTable.add(OLD_MCC);
        columnsForEventTable.add(OLD_MNC);
        columnsForEventTable.add(OLD_RAC);
        columnsForEventTable.add(OLD_LAC);
        columnsForEventTable.add(TRANSFERRED_PDP);
        columnsForEventTable.add(DROPPED_PDP);
        columnsForEventTable.add(HLR);
        columnsForEventTable.add(DURATION);


        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForEventTable);

    }

    private void createLteEventRawErrTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        
        columnsForEventTable.add(BEARER_CAUSE_3);
        columnsForEventTable.add(BEARER_CAUSE_2);
        columnsForEventTable.add(BEARER_CAUSE_1);
        
        columnsForEventTable.add(ARP_PVI_3);
        columnsForEventTable.add(ARP_PVI_2);
        columnsForEventTable.add(ARP_PVI_1);
        columnsForEventTable.add(ARP_PCI_3);
        columnsForEventTable.add(ARP_PCI_2);
        columnsForEventTable.add(ARP_PCI_1);
        columnsForEventTable.add(COMBINED_TAU_TYPE);

        columnsForEventTable.add(SMS_ONLY);

        columnsForEventTable.add(EVENT_SUBTYPE_ID);
        columnsForEventTable.add(L_DISCONNECT_PDN_TYPE);

        columnsForEventTable.add(MCC);
        columnsForEventTable.add(MNC);

        columnsForEventTable.add(QCI_SUC_10);
        columnsForEventTable.add(QCI_SUC_9);
        columnsForEventTable.add(QCI_SUC_8);
        columnsForEventTable.add(QCI_SUC_7);
        columnsForEventTable.add(QCI_SUC_6);
        columnsForEventTable.add(QCI_SUC_5);
        columnsForEventTable.add(QCI_SUC_4);
        columnsForEventTable.add(QCI_SUC_3);
        columnsForEventTable.add(QCI_SUC_2);
        columnsForEventTable.add(QCI_SUC_1);
        
        
        columnsForEventTable.add(QCI_ERR_10);
        columnsForEventTable.add(QCI_ERR_9);
        columnsForEventTable.add(QCI_ERR_8);
        columnsForEventTable.add(QCI_ERR_7);
        columnsForEventTable.add(QCI_ERR_6);
        columnsForEventTable.add(QCI_ERR_5);
        columnsForEventTable.add(QCI_ERR_4);
        columnsForEventTable.add(QCI_ERR_3);
        columnsForEventTable.add(QCI_ERR_2);
        columnsForEventTable.add(QCI_ERR_1);
        columnsForEventTable.add(EPS_BEARER_ID_3);
        columnsForEventTable.add(EPS_BEARER_ID_2);
        columnsForEventTable.add(EPS_BEARER_ID_1);
        columnsForEventTable.add(GBR_DOWNLINK_3);
        columnsForEventTable.add(GBR_DOWNLINK_2);
        columnsForEventTable.add(GBR_DOWNLINK_1);
        columnsForEventTable.add(GBR_UPLINK_3);
        columnsForEventTable.add(GBR_UPLINK_2);
        columnsForEventTable.add(GBR_UPLINK_1);
        columnsForEventTable.add(ARP_PL_3);
        columnsForEventTable.add(ARP_PL_2);
        columnsForEventTable.add(ARP_PL_1);
        columnsForEventTable.add(PDN_PGW_IPV6_3);
        columnsForEventTable.add(PDN_PGW_IPV6_2);
        columnsForEventTable.add(PDN_PGW_IPV6_1);
        columnsForEventTable.add(PDN_PGW_IPV4_3);
        columnsForEventTable.add(PDN_PGW_IPV4_2);
        columnsForEventTable.add(PDN_PGW_IPV4_1);
        columnsForEventTable.add(PDN_PAA_IPV6_3);
        columnsForEventTable.add(PDN_PAA_IPV6_2);
        columnsForEventTable.add(PDN_PAA_IPV6_1);
        columnsForEventTable.add(PDN_PAA_IPV4_3);
        columnsForEventTable.add(PDN_PAA_IPV4_2);
        columnsForEventTable.add(PDN_PAA_IPV4_1);
        columnsForEventTable.add(PDN_BEARER_ID_3);
        columnsForEventTable.add(PDN_BEARER_ID_2);
        columnsForEventTable.add(PDN_BEARER_ID_1);
        columnsForEventTable.add(OLD_SGW_IPV6);
        columnsForEventTable.add(OLD_SGW_IPV4);
        columnsForEventTable.add(OLD_L_MTMSI);
        columnsForEventTable.add(OLD_L_MMEC);
        columnsForEventTable.add(OLD_L_MMEGI);
        columnsForEventTable.add(OLD_CELL_ID);
        columnsForEventTable.add(OLD_TRAC);
        columnsForEventTable.add(TRAC);
        columnsForEventTable.add(MSISDN);
        columnsForEventTable.add(RAT);
        columnsForEventTable.add(SUBCAUSE_CODE);
        columnsForEventTable.add(SERVICE_REQ_TRIGGER);
        columnsForEventTable.add(DETACH_TYPE);
        columnsForEventTable.add(DETACH_TRIGGER);
        columnsForEventTable.add(DEACTIVATION_TRIGGER);
        columnsForEventTable.add(CAUSE_CODE_COL);
        columnsForEventTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForEventTable.add(ATTACH_TYPE);
        columnsForEventTable.add(EVENT_RESULT);
        columnsForEventTable.add(ROAMING);
        columnsForEventTable.add(EVENT_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(EVENT_SOURCE_NAME);
        columnsForEventTable.add(HIERARCHY_3);
        columnsForEventTable.add(HIERARCHY_2);
        columnsForEventTable.add(HIERARCHY_1);
        columnsForEventTable.add(VENDOR);
        columnsForEventTable.add(PAGING_ATTEMPTS);
        columnsForEventTable.add(REQUEST_RETRIES);
        columnsForEventTable.add(APN);
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(IMSI_MNC);
        columnsForEventTable.add(OLD_MCC);
        columnsForEventTable.add(OLD_MNC);
        columnsForEventTable.add(DURATION);


        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForEventTable);

    }
    
    
    



    private void seedTacTable() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();

        valuesForTable.clear();
        valuesForTable.put(TAC, TEST_VALUE_EXCLUSIVE_TAC);
        valuesForTable.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP_NAME);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTable);

    }

}
