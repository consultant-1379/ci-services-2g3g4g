/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.kpi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class KPIAnalysisBySGSNTest extends BaseKPIAnalysisTest {

    @Test
    public void testGetKPIRawDataWhenOnlyOneSuccessEventExists() throws Exception {
        final String dateTime = populateRawTemporaryTablesWithJustOneSuccessAndOneFailEvent(3);
        final String json = getData(FIVE_MINUTES, TYPE_SGSN);
        validateRawResultForOneSuccessAndOneFailEvent(json, dateTime);
    }

    @Test
    public void testGetKPIRawDataWithManyEvents() throws Exception {
        final String dateTime = populateRawTemporaryTablesWithManyEvents(4);
        final String json = getData(FIVE_MINUTES, TYPE_SGSN);
        validateRawResultForForManyEvents(json, dateTime);
    }

    @Test
    public void testGetKPIAggregatedDataWhenManyEventsExist() throws Exception {
        jndiProperties.setUpJNDIPropertiesForTest();
        final String dateTime = populateRawTemporaryTablesWithManyEvents(5);
        populateAggregatedTemporaryTablesWithManyEvents(dateTime);
        final String json = getData(THIRTY_MINUTES, TYPE_SGSN);
        validateAggResultForManyEvents(json, dateTime, 30);
    }

    @Test
    public void testGetKPIDataWithDataTieringWhenManyEventsExist() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        final String dateTime = populateRawTemporaryTablesWithManyEvents(getNumberOfMinutesAgoForRawEvents());
        populateAggregatedTemporaryTablesWithManyEvents(dateTime);
        final String json = getData(THIRTY_MINUTES, TYPE_SGSN);
        validateAggResultForManyEvents(json, dateTime, 2);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetKPIRawDataWithManyEventsWithTACExclusion() throws Exception {
        jndiProperties.setUpJNDIPropertiesForTest();
        final String dateTime = populateRawTemporaryTablesWithManyEvents(4);
        addTACtoTACGroup(SAMPLE_TAC, EXCLUSIVE_TAC_GROUP);
        final String json = getData(FIVE_MINUTES, TYPE_SGSN);
        validateRawResultIsAllZerosForForManyEvents(json, dateTime);
    }

    @Test
    public void testGetKPIAggregatedDataWhenOnlyOneSuccessEventExists() throws Exception {
        jndiProperties.setUpJNDIPropertiesForTest();
        final String dateTime = populateRawTemporaryTablesWithJustOneSuccessAndOneFailEvent(33);
        final String json = getData(THIRTY_MINUTES, TYPE_SGSN);
        validateAggResultForOneSuccessAndOneFailEvent(json, dateTime, 30);
    }

    protected String getData(final String time, final String type) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(NODE_PARAM, SAMPLE_MME);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "500");

        return runGetDataQuery(kpiResource, map);
    }

    private int getNumberOfMinutesAgoForRawEvents() {
        final Date d = new Date();
        final Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.add(Calendar.MINUTE, -25);
        final int i = c.get(Calendar.MINUTE) % 15 + 25;
        return i;
    }

    public String alignToPrevious15MinRop(final String dateTime) {
        final Calendar c = Calendar.getInstance();

        Date d = null;
        SimpleDateFormat formatter = null;
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            d = formatter.parse(dateTime);
        } catch (final ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        c.setTime(d);
        final int currentMinute = c.get(Calendar.MINUTE);
        final int minutesToLastRop = currentMinute % 15;
        c.add(Calendar.MINUTE, -minutesToLastRop);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        return formatter.format(c.getTime());
    }
}
