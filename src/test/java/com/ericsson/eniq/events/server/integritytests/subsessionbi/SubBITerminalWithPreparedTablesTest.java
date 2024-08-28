/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.LookupTechPackPopulator;
import com.ericsson.eniq.events.server.test.queryresults.SubBITerminalResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DIM_E_SGEH_TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER_FOR_SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER_FOR_SAMPLE_TAC_2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MARKETING_NAME_FOR_SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MARKETING_NAME_FOR_SAMPLE_TAC_2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_BAND;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC_2;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_TAC;
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
public class SubBITerminalWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBITerminalResult> {

    private static final Map<String, String> groupColumnsForIMSIGroupTable = new HashMap<String, String>();

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();

    private static final Map<String, Object> baseValues = new HashMap<String, Object>();

    private SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private static final String TEST_TIME = "30";

    private static final String TEST_GROUP_NAME = "VIP";

    private static final String TEST_TZ_OFFSET = "+0000";

    private static final String TEST_MAX_ROWS = "50";

    private final static long TEST_IMSI = 312030410000004L;

    private final static long TEST_IMEISV = 1234567982131400L;

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

        groupColumnsForIMSIGroupTable.put("IMSI", "unsigned bigint");
        groupColumnsForIMSIGroupTable.put("GROUP_NAME", "varchar(64)");

        rawTableColumns.put("TAC", "unsigned int");
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("IMEISV", "unsigned bigint");

        baseValues.put("IMEISV", TEST_IMEISV);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_TAC);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_TAC);
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, groupColumnsForIMSIGroupTable);

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    private void populateTemporaryTables() throws SQLException {
        final SQLExecutor sqlExecutor = null;
        try {
            // Populate SGEH tables
            Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
            rowSpecificValues.putAll(baseValues);

            rowSpecificValues.put("TAC", SAMPLE_TAC);
            rowSpecificValues.put("IMSI", TEST_IMSI);
            SGEH_LAST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -14);
            SGEH_FIRST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -23);
            rowSpecificValues.put("DATETIME_ID", SGEH_FIRST_SEEN);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -17));
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", SGEH_LAST_SEEN);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rowSpecificValues);

            // Populate LTE tables
            rowSpecificValues.put("TAC", SAMPLE_TAC_2);
            rowSpecificValues.put("IMSI", TEST_IMSI_LTE);
            LTE_LAST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -14);
            LTE_FIRST_SEEN = DateTimeUtilities.getDateTime(Calendar.MINUTE, -26);

            rowSpecificValues.put("DATETIME_ID", LTE_FIRST_SEEN);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -18));
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", LTE_LAST_SEEN);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            // Populate imsi group table
            rowSpecificValues = new HashMap<String, Object>();
            rowSpecificValues.put("GROUP_NAME", TEST_GROUP_NAME);
            rowSpecificValues.put("IMSI", TEST_IMSI);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);
            rowSpecificValues.put("IMSI", TEST_IMSI_LTE);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.SubsessionBIResource#getSubBITerminalData()}.
     */
    @Test
    public void testGetSubBITerminalDataGroup() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validateResultsSGEH(results.get(1));
        validateResultsLTE(results.get(0));
    }

    @Test
    public void testGetSubBITerminalData() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validateResultsLTE(results.get(0));
    }

    private void validateResultsSGEH(final SubBITerminalResult result) {
        final SubBITerminalResult expected = new SubBITerminalResult();

        expected.setImsi("312030410000004");
        expected.setImeisv(Long.toString(TEST_IMEISV));
        expected.setTotalEvents("3");
        expected.setManufacturer(MANUFACTURER_FOR_SAMPLE_TAC);
        expected.setMarketingName(MARKETING_NAME_FOR_SAMPLE_TAC);
        expected.setTac(SAMPLE_TAC);
        expected.setFirstSeen(SGEH_FIRST_SEEN + ".0");
        expected.setLastSeen(SGEH_LAST_SEEN + ".0");
        expected.setFailureCount("2");
        expected.setSuccessCount("1");
        expected.setCapability(SAMPLE_BAND);
        assertEquals(expected, result);

    }

    private void validateResultsLTE(final SubBITerminalResult result) {

        final SubBITerminalResult expected = new SubBITerminalResult();
        expected.setImsi(Long.toString(TEST_IMSI_LTE));
        expected.setManufacturer(MANUFACTURER_FOR_SAMPLE_TAC_2);
        expected.setMarketingName(MARKETING_NAME_FOR_SAMPLE_TAC_2);
        expected.setTac(SAMPLE_TAC_2);
        expected.setCapability(SAMPLE_BAND);
        expected.setImeisv(Long.toString(TEST_IMEISV));
        expected.setFailureCount("1");
        expected.setSuccessCount("2");
        expected.setTotalEvents("3");
        expected.setFirstSeen(LTE_FIRST_SEEN + ".0");
        expected.setLastSeen(LTE_LAST_SEEN + ".0");

        assertEquals(expected, result);

    }
}
