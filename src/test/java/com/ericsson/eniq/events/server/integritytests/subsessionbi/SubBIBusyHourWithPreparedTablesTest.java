/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.CHART_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_IMSI;
import static org.junit.Assert.assertEquals;

/**
 * @author ericker
 * @since 2011
 *
 */
public class SubBIBusyHourWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

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

        rawTableColumns.put("EVENT_ID", "tinyint");
        rawTableColumns.put("HOUR_ID", "tinyint");
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
    public void testSubBIBusyHour() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(TYPE_PARAM, "IMSI");
        map.putSingle(IMSI_PARAM, "312030410000004");
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyHourData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResults(results);
    }

    @Test
    public void testSubBIBusyHourGroup() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "1440");
        map.putSingle(GROUP_NAME_PARAM, "Test_Group");
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyHourData();
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
            if (i == 2 || i == 7) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel("" + i);
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(i));
            } else if (i == 3 || i == 8) {
                expectedResultsTwoSuccessOneFailure.setXAxisLabel("" + i);
                assertEquals(expectedResultsTwoSuccessOneFailure, results.get(i));
            } else {
                expectedResultsZero.setXAxisLabel("" + i);
                assertEquals(expectedResultsZero, results.get(i));
            }
        }
    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            Map<String, Object> values = new HashMap<String, Object>();
            values.put("HOUR_ID", 1);
            values.put("EVENT_ID", 1);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(1));
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("HOUR_ID", 7);
            values.put("EVENT_ID", 3);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(1));
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("HOUR_ID", 2);
            values.put("EVENT_ID", 12);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(1));
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

            values = new HashMap<String, Object>();
            values.put("HOUR_ID", 6);
            values.put("EVENT_ID", 13);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(1));
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
