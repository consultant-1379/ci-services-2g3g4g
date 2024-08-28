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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;

import org.junit.Test;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class EventVolumeCellWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    private final static int FIFTY_FIVE_MINS_AGO = 55;

    private final static int TWENTY_FOUR_TIME_SLOTS = 24;//for this test the data sampling is every 12 minutes

    private final static String SIX_HOURS = "360";

    private final static String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Test
    public void testGetEventVolumeData_5Minutes() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        final String result = getCellData(FIVE_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumeData_5Minutes_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getCellData(FIVE_MINUTES);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_6Hours() throws Exception {
        populateTemporaryTablesWithDataSet();
        final String result = getCellData("120");
        validateResults_6Hours(result);
    }

    private String getCellData(final String time) {
        return getData(time, TYPE_CELL, "00,,BSC1,Ericsson,GSM");
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
        columnsForAggTables1.add(HIERARCHY_1);
        columnsForAggTables1.add(HIERARCHY_3);

        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN);
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
        values.put(HIERARCHY_1, "00");
        values.put(HIERARCHY_3, "BSC1");
        insertRow(table, values);
    }

    /**
     * due to data sampling it is very difficult to predict which time slots will be chosen. Therefore the easiest 
     * solution is to place events for each minute between the times of interest. 
     * @throws SQLException
     */
    protected void populateTemporaryTablesWithDataSet() throws SQLException {
        final Map<Integer, String> dateTimeMap = getDateTimeMap(FIFTY_FIVE_MINS_AGO, TWENTY_FOUR_TIME_SLOTS);
        for (int noMinsAgo = FIFTY_FIVE_MINS_AGO; noMinsAgo < (FIFTY_FIVE_MINS_AGO + TWENTY_FOUR_TIME_SLOTS); noMinsAgo++) {
            for (final String rawTable : rawTables) {
                for (final int eventID : eventIDs) {
                    insertRowIntoRawTable(rawTable, eventID, dateTimeMap.get(noMinsAgo));
                }
            }
            for (final String aggTable : aggTables) {
                for (final int eventID : eventIDs) {
                    insertRowIntoAggTable(aggTable, eventID, dateTimeMap.get(noMinsAgo));
                }
            }
        }
    }

    /**
     * Create a map of date time strings based on the number of minutes ago.  Can't use the existing methods 
     * in DateTimeUtilities as they get a new Date object each time which may result in time having moved to the 
     * next minute.
     * 
     * @param startMinsAgo
     * @param noTimeSlots
     * @return
     */
    private Map<Integer, String> getDateTimeMap(final int startMinsAgo, final int noTimeSlots) {
        final DateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        final Date date = new Date();

        // Make sure we're always in GMT so our tests dont fail when timezone changes!
        final Calendar calendar = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
        dateFormat.setCalendar(calendar);

        final Map<Integer, String> dateTimeMap = new HashMap<Integer, String>();

        for (int minsAgo = startMinsAgo; minsAgo < (startMinsAgo + noTimeSlots); minsAgo++) {
            calendar.setTime(date);
            calendar.add(Calendar.MINUTE, -minsAgo);
            dateTimeMap.put(minsAgo, dateFormat.format(calendar.getTime()));
        }

        return dateTimeMap;
    }

}
