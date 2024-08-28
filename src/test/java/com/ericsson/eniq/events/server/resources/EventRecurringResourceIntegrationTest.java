/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * 
 * @author eavidat
 * @since  Jan 2011
 */

public class EventRecurringResourceIntegrationTest extends DataServiceBaseTestCase {

    private static final String TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR = "+0100";

    private MultivaluedMap<String, String> map;

    private EventRecurringResource eventRecurringResource;

    private static final String IMSI = "460000661409028";

    private static final String PTMSI = "661409";

    private static final String GROUP_NAME = "group";

    private static final String TYPE_IMSI = "IMSI";

    private static final String CAUSE_CODE = "1";

    private static final String SUB_CAUSE_CODE = "1";

    private static final String EVENT_RESULT = "1";

    private static final String CAUSE_PROTTYPE = "1";

    private static final String EVENT_TYPE = "ATTACH";

    private static final String CAUSE_PROT_TYPE_HEADER_VALUE = "GTP";

    private static final String CAUSE_CODE_HEADER_VALUE = "Sub Cause Code";

    private static final String SUB_CAUSE_CODE_HEADER_VALUE = "Cause Code";

    private static final String ONE_WEEK_IN_MINUTES = "10080";

    private static final String MAX_ROWS_VALUE = "500";

    private static final String INVALID_IMSI = "1234567891234567891";

    private static final String INVALID_PTMSI = "01234a";

    private static final String INVALID_GROUP_NAME = "group+007";

    @Override
    public void onSetUp() {
        eventRecurringResource = new EventRecurringResource();
        attachDependencies(eventRecurringResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testMissingTypeParam() throws Exception {
        map.clear();
        map.putSingle(IMSI_PARAM, IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingCauseCodeParam() throws Exception {
        map.clear();

        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingSubCauseCodeParam() throws Exception {
        map.clear();

        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingEventTypeParam() throws Exception {
        map.clear();

        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingCauseProtTypeParam() throws Exception {
        map.clear();

        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testMissingEventResultParam() throws Exception {
        map.clear();

        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1100");
        map.putSingle(TIME_TO_QUERY_PARAM, "1200");
        map.putSingle(DATE_FROM_QUERY_PARAM, "14102010");
        map.putSingle(DATE_TO_QUERY_PARAM, "14102010");

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONErrorResult(result);
        assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

    @Test
    public void testGetDataForDatesWithNoRawTablesAvailableReturnsEmptyDataSet() throws Exception {
        map.clear();

        map.putSingle(GROUP_NAME_PARAM, GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_FROM_QUERY_PARAM, "1015");
        map.putSingle(TIME_TO_QUERY_PARAM, "1030");
        final String someDateInTheDistantPast = "11062000";
        map.putSingle(DATE_FROM_QUERY_PARAM, someDateInTheDistantPast);
        map.putSingle(DATE_TO_QUERY_PARAM, someDateInTheDistantPast);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testInvalidIMSIParam() throws Exception {
        map.clear();

        map.putSingle(IMSI_PARAM, INVALID_IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONErrorResult(result);
    }

    @Test
    public void testInvalidPTMSIParam() throws Exception {
        map.clear();

        map.putSingle(PTMSI_PARAM, INVALID_PTMSI);
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONErrorResult(result);
    }

    @Test
    public void testInvalidGroupNameParam() throws Exception {
        map.clear();

        map.putSingle(GROUP_NAME_PARAM, INVALID_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONErrorResult(result);
    }

    @Test
    public void testGetData() throws Exception {
        map.clear();
        map.putSingle(GROUP_NAME_PARAM, GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        try {
            eventRecurringResource.getData();
        } catch (final UnsupportedOperationException e) {
            assertFalse(false);
            return;
        }

        assertFalse(true);
    }

    @Test
    public void testGetRecurErrorSummaryDataTypeIMSI() throws Exception {
        map.clear();
        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(CAUSE_PROT_TYPE_HEADER, CAUSE_PROT_TYPE_HEADER_VALUE);
        map.putSingle(CAUSE_CODE_HEADER, CAUSE_CODE_HEADER_VALUE);
        map.putSingle(SUB_CAUSE_CODE_HEADER, SUB_CAUSE_CODE_HEADER_VALUE);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRecurErrorTotalDataTypeIMSI() throws Exception {
        map.clear();
        map.putSingle(IMSI_PARAM, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRecurErrorSummaryDataTypePTMSI() throws Exception {
        map.clear();
        map.putSingle(PTMSI_PARAM, PTMSI);
        map.putSingle(IMSI_HEADER, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(CAUSE_PROT_TYPE_HEADER, CAUSE_PROT_TYPE_HEADER_VALUE);
        map.putSingle(CAUSE_CODE_HEADER, CAUSE_CODE_HEADER_VALUE);
        map.putSingle(SUB_CAUSE_CODE_HEADER, SUB_CAUSE_CODE_HEADER_VALUE);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRecurErrorTotalDataTypePTMSI() throws Exception {
        map.clear();
        map.putSingle(PTMSI_PARAM, PTMSI);
        map.putSingle(IMSI_HEADER, IMSI);
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRecurErrorSummaryDataIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(GROUP_NAME_PARAM, GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(CAUSE_PROT_TYPE_HEADER, CAUSE_PROT_TYPE_HEADER_VALUE);
        map.putSingle(CAUSE_CODE_HEADER, CAUSE_CODE_HEADER_VALUE);
        map.putSingle(SUB_CAUSE_CODE_HEADER, SUB_CAUSE_CODE_HEADER_VALUE);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorSummaryData();

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRecurErrorTotalDataIMSIGroup() throws Exception {
        map.clear();
        map.putSingle(GROUP_NAME_PARAM, GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);

        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(EVENT_TYPE_PARAM, EVENT_TYPE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROTTYPE);
        map.putSingle(EVENT_RESULT_PARAM, EVENT_RESULT);

        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK_IN_MINUTES);

        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);

        DummyUriInfoImpl.setUriInfo(map, eventRecurringResource);
        final String result = eventRecurringResource.getRecurErrorTotalData();

        assertJSONSucceeds(result);
    }
}