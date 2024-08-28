/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class EventVolumeSGSNWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    @Test
    public void testGetEventVolumeData_5Minutes() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        final String result = getSGSNData(FIVE_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumeData_5Minutes_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getSGSNData(FIVE_MINUTES);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_30Minutes() throws Exception {
        populateTemporaryTablesWithThirtyMinDataSet();
        final String result = getSGSNData(THIRTY_MINUTES);
        validateResults(result);
    }

    private String getSGSNData(final String time) {
        return getData(time, TYPE_SGSN, SAMPLE_SGSN);
    }

    @Override
    protected Collection<String> getColumnsForAggTables() {
        final Collection<String> columnsForAggTables1 = new ArrayList<String>();
        columnsForAggTables1.add(EVENT_ID);
        columnsForAggTables1.add(NO_OF_ERRORS);
        columnsForAggTables1.add(NO_OF_SUCCESSES);
        columnsForAggTables1.add(EVENT_SOURCE_NAME);
        columnsForAggTables1.add(DATETIME_ID);
        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_1MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_1MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_1MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_1MIN);
        return aggTables1;
    }

    @Override
    protected void insertRowIntoAggTable(final String table, final int eventId, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_SUCCESSES, ONE_SUCCESS_EVENT);
        values.put(NO_OF_ERRORS, ONE_ERROR_EVENT);
        insertRow(table, values);
    }

}
