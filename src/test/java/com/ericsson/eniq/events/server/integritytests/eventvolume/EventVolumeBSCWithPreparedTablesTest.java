/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventvolume;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class EventVolumeBSCWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    @Test
    public void testGetEventVolumeData_5Minutes() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        final String result = getBSCData(FIVE_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumeData_5Minutes_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getBSCData(FIVE_MINUTES);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_30Minutes() throws Exception {
        populateTemporaryTablesWithThirtyMinDataSet();
        final String result = getBSCData(THIRTY_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumnDataWithDataTiering_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithThirtyMinDataSet();
        final String result = getBSCData(THIRTY_MINUTES);
        System.err.println(result);
        validateResults(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private String getBSCData(final String time) {
        return getData(time, TYPE_BSC, "BSC1,Ericsson,GSM");
    }

    @Override
    protected Collection<String> getColumnsForAggTables() {
        final Collection<String> columnsForAggTables1 = new ArrayList<String>();
        columnsForAggTables1.add(EVENT_ID);
        columnsForAggTables1.add(NO_OF_ERRORS);
        columnsForAggTables1.add(NO_OF_SUCCESSES);
        columnsForAggTables1.add(DATETIME_ID);
        columnsForAggTables1.add(RAT);
        columnsForAggTables1.add(VENDOR);
        columnsForAggTables1.add(HIERARCHY_3);

        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_15MIN);
        return aggTables1;
    }

    @Override
    protected void insertRowIntoAggTable(final String table, final int eventId, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_SUCCESSES, ONE_SUCCESS_EVENT);
        values.put(NO_OF_ERRORS, ONE_ERROR_EVENT);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, "BSC1");
        insertRow(table, values);
    }

}
