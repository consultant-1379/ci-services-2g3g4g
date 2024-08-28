/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.causecode.details;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.CauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.RawTablesPopulator;
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeEventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisDetailedWithPreparedTablesAPNTest extends
        TestsWithTemporaryTablesBaseTestCase<CauseCodeEventAnalysisDetailedResult> {

    private final CauseCodeAnalysisResource causeCodeAnalysisResource = new CauseCodeAnalysisResource();

    private final static int CAUSE_CODE_VALUE = 3;

    private final static int SUBCAUSE_CODE_VALUE = 10;

    private final static int CAUSE_PROT_TYPE_VALUE = 0;

    private final static String expectedLTESubcauseCodeDescription = "Auth failed";

    private final static String expectedLTESubcauseCodeHelp = "";

    private final static Map<Integer, String> sgehCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> sgehSubCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteSubCauseCodeMapping = new HashMap<Integer, String>();

    @BeforeClass
    static public void setupBeforeClass() {

        sgehCauseCodeMapping.put(CAUSE_CODE_VALUE, "ILLEGAL_MS");
        lteCauseCodeMapping.put(CAUSE_CODE_VALUE,
                "This cause is sent when the PDN connection is released due to LTE generated reasons.");
        sgehSubCauseCodeMapping.put(SUBCAUSE_CODE_VALUE, "Auth failed");
        lteSubCauseCodeMapping.put(SUBCAUSE_CODE_VALUE, "Auth failed");
    }

    @Test
    public void testGetDetailedDataForAPN1WeekLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTimeMinus48Hours());

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(CAUSE_CODE_VALUE));
        map.putSingle(SUB_CAUSE_CODE_PARAM, Integer.toString(SUBCAUSE_CODE_VALUE));
        map.putSingle(CAUSE_PROT_TYPE, Integer.toString(CAUSE_PROT_TYPE_VALUE));
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
    }

    @Test
    public void testGetDetailedDataForAPN1DayLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.HOUR, -12));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(CAUSE_CODE_VALUE));
        map.putSingle(SUB_CAUSE_CODE_PARAM, Integer.toString(SUBCAUSE_CODE_VALUE));
        map.putSingle(CAUSE_PROT_TYPE, Integer.toString(CAUSE_PROT_TYPE_VALUE));
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
    }

    @Test
    public void testGetDetailedDataForAPN1HourLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(CAUSE_CODE_VALUE));
        map.putSingle(SUB_CAUSE_CODE_PARAM, Integer.toString(SUBCAUSE_CODE_VALUE));
        map.putSingle(CAUSE_PROT_TYPE, Integer.toString(CAUSE_PROT_TYPE_VALUE));
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
    }

    @Test
    public void testGetDetailedDataForAPNGroup1HourLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(CAUSE_CODE_VALUE));
        map.putSingle(SUB_CAUSE_CODE_PARAM, Integer.toString(SUBCAUSE_CODE_VALUE));
        map.putSingle(CAUSE_PROT_TYPE, Integer.toString(CAUSE_PROT_TYPE_VALUE));
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
    }

    @Test
    public void testGetDetailedDataForAPNGroup1DayLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.HOUR, -12));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(CAUSE_CODE_VALUE));
        map.putSingle(SUB_CAUSE_CODE_PARAM, Integer.toString(SUBCAUSE_CODE_VALUE));
        map.putSingle(CAUSE_PROT_TYPE, Integer.toString(CAUSE_PROT_TYPE_VALUE));
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
    }

    @Override
    public void onSetUp() throws Exception {
        attachDependencies(causeCodeAnalysisResource);
        createAndPopulateLookupTables();
    }

    private void createAndPopulateRawTables(final String timestamp) throws Exception {
        final RawTablesPopulator rawTablesPopulator = new RawTablesPopulator();
        final Map<String, Object> valuesToInsertInLTETable = new HashMap<String, Object>();
        valuesToInsertInLTETable.put(EVENT_ID, ATTACH_IN_4G);
        valuesToInsertInLTETable.put(RAT, RAT_FOR_LTE);
        valuesToInsertInLTETable.put(APN, SAMPLE_APN);
        valuesToInsertInLTETable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
        valuesToInsertInLTETable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        valuesToInsertInLTETable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
        rawTablesPopulator.createAndPopulateRawLteErrTable(valuesToInsertInLTETable, timestamp, connection);

        final Map<String, Object> valuesToInsertInSgehTable = new HashMap<String, Object>();
        valuesToInsertInSgehTable.put(EVENT_ID, ATTACH_IN_2G_AND_3G);
        valuesToInsertInSgehTable.put(RAT, RAT_FOR_GSM);
        valuesToInsertInSgehTable.put(APN, SAMPLE_APN);
        valuesToInsertInSgehTable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE); // defined in SGEH and LTE but different description
        valuesToInsertInSgehTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        valuesToInsertInSgehTable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
        rawTablesPopulator.createAndPopulateRawSgehErrTable(valuesToInsertInSgehTable, timestamp, connection);
    }

    private void createAndPopulateRawTablesForGroups(final String timestamp) throws Exception {
        final RawTablesPopulator rawTablesPopulator = new RawTablesPopulator();
        final Map<String, Object> valuesToInsertInLTETable = new HashMap<String, Object>();
        valuesToInsertInLTETable.put(EVENT_ID, ATTACH_IN_4G);
        valuesToInsertInLTETable.put(RAT, RAT_FOR_LTE);
        valuesToInsertInLTETable.put(APN, SAMPLE_APN);
        valuesToInsertInLTETable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
        valuesToInsertInLTETable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        valuesToInsertInLTETable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
        rawTablesPopulator.createAndPopulateRawLteErrTable(valuesToInsertInLTETable, timestamp, connection);

        valuesToInsertInLTETable.clear();
        valuesToInsertInLTETable.put(EVENT_ID, ATTACH_IN_4G);
        valuesToInsertInLTETable.put(RAT, RAT_FOR_LTE);
        valuesToInsertInLTETable.put(APN, SAMPLE_APN2);
        valuesToInsertInLTETable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
        valuesToInsertInLTETable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        valuesToInsertInLTETable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
        rawTablesPopulator.createAndPopulateRawLteErrTable(valuesToInsertInLTETable, timestamp, connection);

        final Map<String, Object> valuesToInsertInSgehTable = new HashMap<String, Object>();
        valuesToInsertInSgehTable.put(EVENT_ID, ATTACH_IN_2G_AND_3G);
        valuesToInsertInSgehTable.put(RAT, RAT_FOR_GSM);
        valuesToInsertInSgehTable.put(APN, SAMPLE_APN);
        valuesToInsertInSgehTable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE); // defined in SGEH and LTE but different description
        valuesToInsertInSgehTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        valuesToInsertInSgehTable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
        rawTablesPopulator.createAndPopulateRawSgehErrTable(valuesToInsertInSgehTable, timestamp, connection);

        valuesToInsertInSgehTable.clear();
        valuesToInsertInSgehTable.put(EVENT_ID, ATTACH_IN_2G_AND_3G);
        valuesToInsertInSgehTable.put(RAT, RAT_FOR_GSM);
        valuesToInsertInSgehTable.put(APN, SAMPLE_APN2);
        valuesToInsertInSgehTable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE); // defined in SGEH and LTE but different description
        valuesToInsertInSgehTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        valuesToInsertInSgehTable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
        rawTablesPopulator.createAndPopulateRawSgehErrTable(valuesToInsertInSgehTable, timestamp, connection);

    }

    private void createAndPopulateLookupTables() throws Exception {
        final List<String> lookupTables = new ArrayList<String>();
        lookupTables.add(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE);
        lookupTables.add(TEMP_DIM_E_SGEH_CAUSECODE);
        lookupTables.add(TEMP_DIM_E_SGEH_SUBCAUSECODE);
        lookupTables.add(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE);
        lookupTables.add(TEMP_DIM_E_LTE_CAUSECODE);
        lookupTables.add(TEMP_DIM_E_LTE_SUBCAUSECODE);

        for (final String lookupTableRequired : lookupTables) {
            createAndPopulateLookupTable(lookupTableRequired);
        }

        final Collection<String> columnsForGroup = new ArrayList<String>();
        columnsForGroup.add(GROUP_NAME);
        columnsForGroup.add(APN);
        createTemporaryTable(TEMP_GROUP_TYPE_E_APN, columnsForGroup);

        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(GROUP_NAME, SAMPLE_APN_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_APN, values);
    }
}
