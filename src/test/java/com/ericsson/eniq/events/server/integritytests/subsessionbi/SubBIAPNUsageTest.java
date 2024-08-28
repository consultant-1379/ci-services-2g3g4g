/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 * @since 2011
 *
 */
public class SubBIAPNUsageTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private static final String TEST_IMSI = "312030410000004";

    SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private final static Map<String, String> rawTableColumns = new HashMap<String, String>();

    private final long testIMSI = Long.valueOf(TEST_IMSI);

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("EVENT_ID", "unsigned bigint");
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("APN", "varchar(127)");
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    @Test
    public void testGetSUBBIAPNUsage_OneWeek() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIAPNData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        validateResults(results);
    }

    private void validateResults(final List<SubBIResult> results) {
        assertThat(results.size(), is(2));
        final SubBIResult firstSubBIResult = results.get(0);
        assertThat(firstSubBIResult.getXAxisLabel(), is(SAMPLE_APN2));
        assertThat(firstSubBIResult.getSuccessCount(), is("5"));
        assertThat(firstSubBIResult.getFailureCount(), is("5"));

        final SubBIResult secondSubBIResult = results.get(1);
        assertThat(secondSubBIResult.getXAxisLabel(), is(SAMPLE_APN));
        assertThat(secondSubBIResult.getSuccessCount(), is("2"));
        assertThat(secondSubBIResult.getFailureCount(), is("1"));

    }

    private void populateTemporaryTables() throws SQLException {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, ATTACH_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, DEDICATED_BEARER_ACTIVATE_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, DEDICATED_BEARER_DEACTIVATE_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, PDN_CONNECT_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, PDN_DISCONNECT_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        /* these are included here and should be excluded by the sql query */
        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, DETACH_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN2);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, DETACH_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, SERVICE_REQUEST_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

    }
}
