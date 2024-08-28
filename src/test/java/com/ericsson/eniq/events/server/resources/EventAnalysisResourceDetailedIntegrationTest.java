/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import java.net.URISyntaxException;
import java.util.StringTokenizer;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.json.JSONArray;
import com.ericsson.eniq.events.server.json.JSONException;
import com.ericsson.eniq.events.server.json.JSONObject;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.util.JSONTestUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static org.junit.Assert.assertTrue;

/**
 * Integration Tests for detailed event analysis queries
 * @author ehaoswa
 * @since  Apr 2010
 */

public class EventAnalysisResourceDetailedIntegrationTest extends BaseServiceIntegrationTest {

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    private static final String TIME_VALUE_OF_1_WEEK = "10080";

    private static final String INDEX_OF_OLD_SGSN_IP_ADDRESS_IN_EVENT_ANALYSIS_DETAILS_QUERY = "23";

    private MultivaluedMap<String, String> map;

    private EventAnalysisService eventAnalysisResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String EVENT_ID = "1";

    private static final String APN = "apn1";

    private static final String ISRAU_EVENT_ID = "3";

    private static final String RAU_EVENT_ID = "2";

    private static final String MAX_ROWS_VALUE = "500";

    private static final String _IMSI = "460030057717063";

    private static final String IMSI = "460000661409028";

    @Before
    public void init() {
        eventAnalysisResource = new EventAnalysisService();
        attachDependencies(eventAnalysisResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    /**
     * All imsi queries are detailed event analysis queries, so belong in this class
     */
    public void testGetEventAnalysisDataByImsiAndTimeOfOneWeek() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testDrillDownByTerminalWith3EventTypesSpecifiedIsAccepted() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35347103");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(EVENT_ID_PARAM, RAU_EVENT_ID + "," + ISRAU_EVENT_ID + ",28");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testDrillDownByTerminalWith2EventTypesSpecifiedIsAccepted() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35331404");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(EVENT_ID_PARAM, RAU_EVENT_ID + "," + ISRAU_EVENT_ID);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testMissingKeyParam() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, _IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = runQuery(map, eventAnalysisResource);
        assertResultContains(result, E_INVALID_VALUES);
    }

    @Test
    public void testMissingKeyAndTypeParam() throws Exception {
        map.clear();
        map.putSingle(IMSI_PARAM, _IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = runQuery(map, eventAnalysisResource);
        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_VALUES);
    }

    @Test
    public void testNoSuchDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID);
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");
        map.putSingle(APN_PARAM, APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = runQuery(map, eventAnalysisResource);
        assertJSONErrorResult(result);
        assertResultContains(result, E_NO_SUCH_DISPLAY_TYPE);
    }

    @Test
    public void testDrillDownByTerminalWithNoEventTypeSpecifiedIsAccepted() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35347103");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetEventAnalysisDataByImsiKeyTotal() throws Exception {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        final String imsi = "460030057717063";
        map.putSingle(IMSI_PARAM, imsi);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetEventAnalysisDataByImsiAndTimeOfOneWeekKeyTotal() throws Exception {
        map.clear();
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, _IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetDrillDownDataByTypeAndTimeOfOneWeek() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(APN_PARAM, APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetDrillDownDataBySGSNEventId2AndTime() throws Exception {
        testGetDrillDownDataBySGSNAndTimeWithEventId("2");
    }

    @Test
    public void testGetDrillDownDataBySGSNEventId3AndTime() throws Exception {
        testGetDrillDownDataBySGSNAndTimeWithEventId("3");
    }

    public void testGetDrillDownDataBySGSNAndTimeWithEventId(final String eventId) throws JSONException,
            URISyntaxException {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(SGSN_PARAM, "SGSN1");
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = runQueryAndAssertJsonSucceeds(map, eventAnalysisResource);
        final JSONArray dataArray = JSONTestUtils.getDataElement(result);
        if (dataArray != null && dataArray.length() > 1) {
            final JSONObject firstRow = (JSONObject) dataArray.get(0);
            final String valueForOldSGSNIPADdress = (String) firstRow
                    .get(INDEX_OF_OLD_SGSN_IP_ADDRESS_IN_EVENT_ANALYSIS_DETAILS_QUERY);
            if (isValueNotEmptyAndLooksLikeIPAddress(valueForOldSGSNIPADdress)) {
                assertTrue("Value of old sgsn field should be in decimal ip address format, value was "
                        + valueForOldSGSNIPADdress, isStringDecimalIPAddress(valueForOldSGSNIPADdress));
            }
        }
    }

    private boolean isValueNotEmptyAndLooksLikeIPAddress(final String valueForOldSGSNIPADdress) {
        return valueForOldSGSNIPADdress.length() > 0 && valueForOldSGSNIPADdress.contains(".");
    }

    private boolean isStringDecimalIPAddress(final String valueForOldSGSNIPADdress) {
        final int numberOfDotsInString = valueForOldSGSNIPADdress.replaceAll("[^.]", "").length();
        if (numberOfDotsInString != 3) {
            return false;
        }
        final StringTokenizer stringTokenizer = new StringTokenizer(valueForOldSGSNIPADdress, ".");
        while (stringTokenizer.hasMoreTokens()) {
            final String ipToken = stringTokenizer.nextToken();
            if (ipToken.length() < 1 || ipToken.length() > 3) {
                return false;
            }
            for (int i = 0; i < ipToken.length(); i++) {
                if (!Character.isDigit(ipToken.charAt(i))) {
                    return false;
                }
            }
        }

        return true;
    }

    @Test
    public void testGetDrillDownDataByTypeAPNAndTime() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(APN_PARAM, APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    private void addTimeParameterToParameterMap() {
        //map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(TIME_QUERY_PARAM, "2440");
    }

    @Test
    public void testGetDetailDataByManufacturer() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(MAN_PARAM, "LG Electronics");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetDetailDataByTerminal() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, "35460200");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(EVENT_ID_PARAM, EVENT_ID);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetDataForDatesWithNoRawTablesAvailableReturnsEmptyDataSet() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(IMSI_PARAM, "123456789012345");
        map.putSingle(TIME_FROM_QUERY_PARAM, "1015");
        map.putSingle(TIME_TO_QUERY_PARAM, "1030");
        final String someDateInTheDistantPast = "11062000";
        map.putSingle(DATE_FROM_QUERY_PARAM, someDateInTheDistantPast);
        map.putSingle(DATE_TO_QUERY_PARAM, someDateInTheDistantPast);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetDetailDataByPTMSI() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        map.putSingle(PTMSI_PARAM, "0");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME_VALUE_OF_1_WEEK);
        map.putSingle(KEY_PARAM, KEY_TYPE_TOTAL);
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

    @Test
    public void testGetTotalDataByMSISDN() {
        map.clear();
        map.putSingle(TIME_FROM_QUERY_PARAM, "0000");
        map.putSingle(TIME_TO_QUERY_PARAM, "0000");
        final String someDateInTheDistantPast = "16052012";
        map.putSingle(DATE_FROM_QUERY_PARAM, someDateInTheDistantPast);
        map.putSingle(DATE_TO_QUERY_PARAM, someDateInTheDistantPast);
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(KEY_PARAM, "TOTAL");
        map.putSingle(TYPE_PARAM, "MSISDN");
        map.putSingle(MSISDN_PARAM, "852972966700000");
        System.out.println(runQueryAndAssertJsonSucceeds(map, eventAnalysisResource));
    }

}
