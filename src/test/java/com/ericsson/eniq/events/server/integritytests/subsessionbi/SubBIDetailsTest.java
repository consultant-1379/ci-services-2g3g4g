/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIDetailsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ericker
 * @since 2011
 * 
 */
public class SubBIDetailsTest extends TestsWithTemporaryTablesBaseTestCase<SubBIDetailsResult> {

    private static final Map<String, String> groupColumns = new HashMap<String, String>();

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();

    private static final Map<String, Object> rawTableValues = new HashMap<String, Object>();

    private static final Map<String, Object> groupTableValues = new HashMap<String, Object>();

    private static final Map<String, String> imsiMsisdnColumns = new HashMap<String, String>();

    private static final Map<String, Object> imsiMsisdnTableValues = new HashMap<String, Object>();

    private SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private static final String TEST_TIME = "30";

    private static final String TEST_GROUP_NAME = "VIP";

    private static final long TEST_MSISDN = 312030410000004L;

    private static final String TEST_TZ_OFFSET = "+0000";

    private static final String TEST_MAX_ROWS = "50";

    private static final String TEST_TAC_SGEH = "102900";

    private static final String TEST_TAC_LTE = "103300";

    private static final String TEST_VENDOR = "Ericsson";

    private final static long TEST_IMSI_SGEH = 312030410000004L;

    private final static String TEST_PTMSI = "908";

    private final static String TEST_EVENT_SOURCE_NAME = "MME2";

    private final static String TEST_HIERARCHY_3 = "BSC56";

    private final static String TEST_HIERARCHY_1 = "CELL11116";

    private final static String TEST_MCC = "460";

    private final static String TEST_MNC = "00";

    private final static int TEST_LAC = 112;

    private final static int TEST_RAC = 1;

    private final static long TEST_IMSI_LTE = 312030419990004L;

    private static String SGEH_FIRST_SEEN = "";

    private static String LTE_FIRST_SEEN = "";

    private static String SGEH_LAST_SEEN = "";

    private static String LTE_LAST_SEEN = "";

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        groupColumns.put("IMSI", "unsigned bigint");
        groupColumns.put("GROUP_NAME", "varchar(64)");

        imsiMsisdnColumns.put("IMSI", "unsigned bigint");
        imsiMsisdnColumns.put("MSISDN", "unsigned bigint");

        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("PTMSI", "unsigned int");
        rawTableColumns.put("ROAMING", "bit");
        rawTableColumns.put("HIERARCHY_1", "varchar(128)");
        rawTableColumns.put("HIERARCHY_3", "varchar(128)");
        rawTableColumns.put("VENDOR", "varchar(20)");
        rawTableColumns.put("RAT", "tinyint");
        rawTableColumns.put("MCC", "varchar(3)");
        rawTableColumns.put("MNC", "varchar(3)");
        rawTableColumns.put("LAC", "unsigned int");
        rawTableColumns.put("RAC", "tinyint");
        rawTableColumns.put("EVENT_SOURCE_NAME", "varchar(128)");
        rawTableColumns.put("IMSI_MCC", "varchar(3)");
        rawTableColumns.put("IMSI_MNC", "varchar(3)");
        rawTableColumns.put("TAC", "unsigned int");

        rawTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinus3Minutes());
        rawTableValues.put("IMSI", TEST_IMSI_SGEH);
        rawTableValues.put("PTMSI", TEST_PTMSI);
        rawTableValues.put("ROAMING", 1);
        rawTableValues.put("HIERARCHY_1", TEST_HIERARCHY_1);
        rawTableValues.put("HIERARCHY_3", TEST_HIERARCHY_3);
        rawTableValues.put("VENDOR", TEST_VENDOR);
        rawTableValues.put("RAT", 1);
        rawTableValues.put("MCC", TEST_MCC);
        rawTableValues.put("MNC", TEST_MNC);
        rawTableValues.put("LAC", TEST_LAC);
        rawTableValues.put("RAC", TEST_RAC);
        rawTableValues.put("EVENT_SOURCE_NAME", TEST_EVENT_SOURCE_NAME);
        rawTableValues.put("IMSI_MCC", TEST_MCC);
        rawTableValues.put("IMSI_MNC", TEST_MNC);
        rawTableValues.put("TAC", TEST_TAC_SGEH);

    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, groupColumns);
        createTemporaryTableWithColumnTypes(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnColumns);

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    private void populateTemporaryTables() throws SQLException {
        // Populate SGEH tables
        SGEH_LAST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -3);
        SGEH_FIRST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -13);

        rawTableValues.put("TAC", TEST_TAC_SGEH);
        rawTableValues.put("IMSI", TEST_IMSI_SGEH);
        rawTableValues.put("PTMSI", TEST_PTMSI);
        rawTableValues.put("RAT", "1");

        rawTableValues.put("DATETIME_ID", SGEH_FIRST_SEEN);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -7));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", SGEH_LAST_SEEN);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rawTableValues);

        // Populate LTE tables
        rawTableValues.put("TAC", TEST_TAC_LTE);
        rawTableValues.put("IMSI", TEST_IMSI_LTE);
        rawTableValues.put("PTMSI", "000");
        rawTableValues.put("RAT", "2");
        LTE_LAST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4);
        LTE_FIRST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -16);

        rawTableValues.put("DATETIME_ID", LTE_FIRST_SEEN);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -8));
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", LTE_LAST_SEEN);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rawTableValues);

        // Populate imsi group table

        groupTableValues.put("GROUP_NAME", TEST_GROUP_NAME);
        groupTableValues.put("IMSI", TEST_IMSI_SGEH);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, groupTableValues);
        groupTableValues.put("IMSI", TEST_IMSI_LTE);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, groupTableValues);

        // Populate imsi group table

        imsiMsisdnTableValues.put("MSISDN", TEST_MSISDN);
        imsiMsisdnTableValues.put("IMSI", TEST_IMSI_SGEH);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);
        imsiMsisdnTableValues.put("IMSI", TEST_IMSI_LTE);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);
    }

    /**
     * Test method for
     * {@link com.ericsson.eniq.events.server.resources.SubsessionBIResource#getSubBITerminalData()}
     * .
     */
    @Test
    public void testGetSubBISubscriberDetailsDataSGEH() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_SGEH));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();
        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsSGEH(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
    }

    @Test
    public void testGetSubBISubscriberDetailsDataLTE() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();
        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsLTE(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
    }

    @Test
    public void testGetSubBISubscriberDetailsDataPTMSI() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(PTMSI_PARAM, TEST_PTMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = false;
        validateResultsPTMSI(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
    }

    /**
     * @param results
     */
    private void validateResultsSGEH(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN));
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }

    private void validateResultsLTE(final SubBIDetailsResult result) {

        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN));
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-2");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_TAC_LTE);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(LTE_FIRST_SEEN + ".0");
        expected.setLastObserved(LTE_LAST_SEEN + ".0");
        expected.setLastObservedPTMSI("");

        assertEquals(expected, result);
    }

    private void validateResultsPTMSI(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus(null);
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }
}
