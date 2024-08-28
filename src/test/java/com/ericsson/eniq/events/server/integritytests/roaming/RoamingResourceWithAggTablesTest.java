/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.roaming;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.test.queryresults.RoamingAnalysisResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

/**
 * @author edivkir
 * @since 2011
 *
 */
public class RoamingResourceWithAggTablesTest extends BaseRoamingResourceWithPreparedTables {

    final String dateTime = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -100);

    /**
     * @throws java.lang.Exception
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        populateTemporaryTables(dateTime);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MCC_MNC_ROAM_15MIN, MCC_FOR_ARGENTINA, MNC_FOR_MOVISTAR, "10", "5",
                dateTime);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MCC_MNC_ROAM_15MIN, MCC_FOR_NORWAY, MNC_FOR_TELENOR_NORWAY, "15", "3",
                dateTime);
    }

    @Test
    public void testRoamingOperatorWithAggTables() throws Exception {

        final MultivaluedMap<String, String> map = getMap();
        DummyUriInfoImpl.setUriInfo(map, roamingResource);

        final String json = roamingResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED", TYPE_ROAMING_OPERATOR,
                map);
        System.out.println(json);

        final List<RoamingAnalysisResult> roamingResult = getTranslator().translateResult(json,
                RoamingAnalysisResult.class);
        assertThat(roamingResult.size(), is(1));

        final RoamingAnalysisResult firstResult = roamingResult.get(0);
        assertThat(firstResult.getOperator(), is(MOVISTAR));
        assertThat(firstResult.getNoErrors(), is(10));
        assertThat(firstResult.getNoSuccesses(), is(5));
        assertThat(firstResult.getRoamingSubscribers(), is(1));
    }

    @Test
    public void testRoamingCountryWithAggTables() throws Exception {

        final MultivaluedMap<String, String> map = getMap();
        DummyUriInfoImpl.setUriInfo(map, roamingResource);

        final String json = roamingResource
                .getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED", TYPE_ROAMING_COUNTRY, map);
        System.out.println(json);

        final List<RoamingAnalysisResult> roamingResult = getTranslator().translateResult(json,
                RoamingAnalysisResult.class);
        assertThat(roamingResult.size(), is(1));

        final RoamingAnalysisResult firstResult = roamingResult.get(0);
        assertThat(firstResult.getOperator(), is(ARGENTINA));
        assertThat(firstResult.getNoErrors(), is(10));
        assertThat(firstResult.getNoSuccesses(), is(5));
        assertThat(firstResult.getRoamingSubscribers(), is(1));
    }

    private void insertRowIntoAggTable(final String table, final String mcc, final String mnc, final String noOfErrors,
            final String noOfSuccesses, final String dateTime1) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI_MCC, mcc);
        values.put(IMSI_MNC, mnc);
        values.put(NO_OF_ERRORS, noOfErrors);
        values.put(NO_OF_SUCCESSES, noOfSuccesses);
        values.put(DATETIME_ID, dateTime1);

        insertRow(table, values);
    }

    @Override
    protected MultivaluedMap<String, String> getMap() {
        final MultivaluedMap<String, String> map = super.getMap();
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        return map;
    }
}
