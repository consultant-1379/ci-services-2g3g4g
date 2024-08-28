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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.PTMSI_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static org.junit.Assert.assertEquals;

public class SubBIFailedEventsWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private final static long testIMSI = 312030410000004L;

    private final static String testPTMSI = "0908";

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        final Map<String, String> rawTableColumns = new HashMap<String, String>();
        rawTableColumns.put("DATE_ID", "date");
        rawTableColumns.put("EVENT_ID", "tinyint");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("PTMSI", "unsigned int");
        rawTableColumns.put("DATETIME_ID", "timestamp");

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }
        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    @Test
    public void testSubBIFailedEvents() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "5");
        map.putSingle(TYPE_PARAM, "IMSI");
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    @Test
    public void testSubBIFailedEventsPTMSI() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "5");
        map.putSingle(TYPE_PARAM, "PTMSI");
        map.putSingle(PTMSI_PARAM, testPTMSI);
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsContainsOnlySGEH(results);
    }

    /**
     * Validates that only SGEH events are present
     * 
     * @param results
     */
    private void validateResultsContainsOnlySGEH(final List<SubBIResult> results) {
        final SubBIResult activateResult = results.get(0);
        assertEquals("1", activateResult.getSuccessCount());
        assertEquals("1", activateResult.getFailureCount());
        assertEquals("ACTIVATE,1", activateResult.getXAxisLabel());

        final SubBIResult israuResult = results.get(1);
        assertEquals("1", israuResult.getSuccessCount());
        assertEquals("1", israuResult.getFailureCount());
        assertEquals("ISRAU,3", israuResult.getXAxisLabel());

        final SubBIResult rauResult = results.get(2);
        assertEquals("1", rauResult.getSuccessCount());
        assertEquals("1", rauResult.getFailureCount());
        assertEquals("RAU,2", rauResult.getXAxisLabel());
    }

    /**
     * Validates that all events are present
     * 
     * @param results
     */
    private void validateResultsContainSGEHAndLTE(final List<SubBIResult> results) {

        final SubBIResult activateResult = results.get(0);
        assertEquals("1", activateResult.getSuccessCount());
        assertEquals("1", activateResult.getFailureCount());
        assertEquals("ACTIVATE,1", activateResult.getXAxisLabel());

        final SubBIResult israuResult = results.get(1);
        assertEquals("1", israuResult.getSuccessCount());
        assertEquals("1", israuResult.getFailureCount());
        assertEquals("ISRAU,3", israuResult.getXAxisLabel());

        final SubBIResult lAttachResult = results.get(2);
        assertEquals("1", lAttachResult.getSuccessCount());
        assertEquals("1", lAttachResult.getFailureCount());
        assertEquals("L_ATTACH,5", lAttachResult.getXAxisLabel());

        final SubBIResult lDetachResult = results.get(3);
        assertEquals("1", lDetachResult.getSuccessCount());
        assertEquals("1", lDetachResult.getFailureCount());
        assertEquals("L_DETACH,6", lDetachResult.getXAxisLabel());

        final SubBIResult lHandoverResult = results.get(4);
        assertEquals("1", lHandoverResult.getSuccessCount());
        assertEquals("1", lHandoverResult.getFailureCount());
        assertEquals("L_HANDOVER,7", lHandoverResult.getXAxisLabel());

        final SubBIResult rauResult = results.get(5);
        assertEquals("1", rauResult.getSuccessCount());
        assertEquals("1", rauResult.getFailureCount());
        assertEquals("RAU,2", rauResult.getXAxisLabel());
    }

    private void insertRow(final SQLExecutor sqlExecutor, final String table, final int eventID,
            final String datetimeID, final long imsi, final String ptmsi, final String date) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values('" + date + "'," + eventID + "," + imsi + ",'"
                + datetimeID + "'," + ptmsi + ")");
    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTimeMinus3Minutes();
            final String date = DateTimeUtilities.getDateMinus3Minutes();

            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_ERR_RAW, 1, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_ERR_RAW, 2, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_ERR_RAW, 3, dateTime, testIMSI, testPTMSI, date);

            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_SUC_RAW, 1, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_SUC_RAW, 2, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_SUC_RAW, 3, dateTime, testIMSI, testPTMSI, date);

            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_ERR_RAW, 5, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_ERR_RAW, 6, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_ERR_RAW, 7, dateTime, testIMSI, testPTMSI, date);

            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_SUC_RAW, 5, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_SUC_RAW, 6, dateTime, testIMSI, testPTMSI, date);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_SUC_RAW, 7, dateTime, testIMSI, testPTMSI, date);
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

}
