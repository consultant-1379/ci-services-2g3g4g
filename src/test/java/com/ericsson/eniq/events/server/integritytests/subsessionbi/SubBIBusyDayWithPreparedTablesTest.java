/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Ignore;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ericker
 * @since 2011
 *
 */
@Ignore
public class SubBIBusyDayWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private final static Map<String, String> rawTableColumns = new HashMap<String, String>();

    private final static Map<String, String> groupColumns = new HashMap<String, String>();

    private final long testIMSI = 312030410000004L;

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("DATETIME_ID", "timestamp");

        groupColumns.put("IMSI", "unsigned bigint");
        groupColumns.put("GROUP_NAME", "varchar(64)");
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, groupColumns);

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    @Test
    public void testSubBIBusyDay() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TYPE_PARAM, "IMSI");
        map.putSingle(IMSI_PARAM, "312030410000004");
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResults(results);
    }

    @Test
    public void testSubBIBusyDayGroup() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(GROUP_NAME_PARAM, "Test_Group");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResults(results);
    }

    /**
     * @param results
     */
    private void validateResults(final List<SubBIResult> results) {
        final SubBIResult expectedResultsZero = new SubBIResult();
        expectedResultsZero.setFailureCount("0");
        expectedResultsZero.setSuccessCount("0");

        final SubBIResult expectedResultsTwoSuccessOneFailure = new SubBIResult();
        expectedResultsTwoSuccessOneFailure.setFailureCount("1");
        expectedResultsTwoSuccessOneFailure.setSuccessCount("2");

        final SubBIResult expectedResultsOneSuccessTwoFailure = new SubBIResult();
        expectedResultsOneSuccessTwoFailure.setFailureCount("2");
        expectedResultsOneSuccessTwoFailure.setSuccessCount("1");

        for (int i = 0; i < results.size(); i++) {
            final String dayOfWeek = DateTimeUtilities.getNameOfDayOfWeek(i + 1);
            if (i + 1 == DateTimeUtilities.getDayOfWeekMinusDay(1)) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(i));
            } else if (i + 1 == DateTimeUtilities.getDayOfWeekMinusDay(4)) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(i));
            } else if (i + 1 == DateTimeUtilities.getDayOfWeekMinusDay(2)) {
                expectedResultsTwoSuccessOneFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsTwoSuccessOneFailure, results.get(i));
            } else if (i + 1 == DateTimeUtilities.getDayOfWeekMinusDay(3)) {
                expectedResultsTwoSuccessOneFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsTwoSuccessOneFailure, results.get(i));
            } else {
                expectedResultsZero.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsZero, results.get(i));
            }
        }
    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            Map<String, Object> values = new HashMap<String, Object>();
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusDay(1));
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusDay(2));
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusDay(3));
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusDay(4));
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("GROUP_NAME", "Test_Group");
            values.put("IMSI", testIMSI);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, values);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

}
