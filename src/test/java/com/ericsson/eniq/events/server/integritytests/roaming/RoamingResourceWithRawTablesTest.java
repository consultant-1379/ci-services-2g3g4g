/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.roaming;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.List;

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
public class RoamingResourceWithRawTablesTest extends BaseRoamingResourceWithPreparedTables {

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        populateTemporaryTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -25));
    }

    @Test
    public void testRoamingOperatorWithRawTables() throws Exception {

        final MultivaluedMap<String, String> map = getMap();
        DummyUriInfoImpl.setUriInfo(map, roamingResource);

        final String json = roamingResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED", TYPE_ROAMING_OPERATOR,
                map);
        System.out.println(json);
        validateResultForRoamingOperator(json);
    }

    @Test
    public void testRoamingOperatorWithDataTieringOn30Min() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateAggData(DateTimeUtilities.getDateTimeMinus25Minutes());
        final MultivaluedMap<String, String> map = getMap();
        DummyUriInfoImpl.setUriInfo(map, roamingResource);

        final String json = roamingResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED", TYPE_ROAMING_OPERATOR,
                map);
        System.out.println(json);
        final List<RoamingAnalysisResult> roamingResult = getTranslator().translateResult(json,
                RoamingAnalysisResult.class);
        assertThat(roamingResult.size(), is(2));

        final RoamingAnalysisResult firstResult = roamingResult.get(0);
        assertThat(firstResult.getOperator(), is(MOVISTAR));
        assertThat(firstResult.getNoErrors(), is(2));
        assertThat(firstResult.getNoSuccesses(), is(0));
        assertThat(firstResult.getRoamingSubscribers(), is(1));

        final RoamingAnalysisResult secondResult = roamingResult.get(1);
        assertThat(secondResult.getOperator(), is(T_MOBILE));
        assertThat(secondResult.getNoErrors(), is(1));
        assertThat(secondResult.getNoSuccesses(), is(10));
        assertThat(secondResult.getRoamingSubscribers(), is(1));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testRoamingCountryWithRawTables() throws Exception {

        final MultivaluedMap<String, String> map = getMap();
        DummyUriInfoImpl.setUriInfo(map, roamingResource);

        final String json = roamingResource
                .getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED", TYPE_ROAMING_COUNTRY, map);
        System.out.println(json);

        validateResultForRoamingCountry(json);
    }

    @Test
    public void testRoamingCountryWithDataTieringOn30Min() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();

        populateAggData(DateTimeUtilities.getDateTimeMinus25Minutes());

        final MultivaluedMap<String, String> map = getMap();
        DummyUriInfoImpl.setUriInfo(map, roamingResource);

        final String json = roamingResource
                .getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED", TYPE_ROAMING_COUNTRY, map);
        System.out.println(json);

        final List<RoamingAnalysisResult> roamingResult = getTranslator().translateResult(json,
                RoamingAnalysisResult.class);
        assertThat(roamingResult.size(), is(2));

        final RoamingAnalysisResult firstResult = roamingResult.get(0);
        assertThat(firstResult.getOperator(), is(ARGENTINA));
        assertThat(firstResult.getNoErrors(), is(2));
        assertThat(firstResult.getNoSuccesses(), is(0));
        assertThat(firstResult.getRoamingSubscribers(), is(1));

        final RoamingAnalysisResult secondResult = roamingResult.get(1);
        assertThat(secondResult.getOperator(), is(USA));
        assertThat(secondResult.getNoErrors(), is(1));
        assertThat(secondResult.getNoSuccesses(), is(10));
        assertThat(secondResult.getRoamingSubscribers(), is(1));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResultForRoamingOperator(final String json) throws Exception {
        final List<RoamingAnalysisResult> roamingResult = getTranslator().translateResult(json,
                RoamingAnalysisResult.class);
        assertThat(roamingResult.size(), is(3));

        final RoamingAnalysisResult firstResult = roamingResult.get(0);
        assertThat(firstResult.getOperator(), is(MOVISTAR));
        assertThat(firstResult.getNoErrors(), is(2));
        assertThat(firstResult.getNoSuccesses(), is(0));
        assertThat(firstResult.getRoamingSubscribers(), is(1));

        final RoamingAnalysisResult secondResult = roamingResult.get(1);
        assertThat(secondResult.getOperator(), is(T_MOBILE));
        assertThat(secondResult.getNoErrors(), is(1));
        assertThat(secondResult.getNoSuccesses(), is(1));
        assertThat(secondResult.getRoamingSubscribers(), is(1));

        final RoamingAnalysisResult thirdResult = roamingResult.get(2);
        assertThat(thirdResult.getOperator(), is(TELENOR));
        assertThat(thirdResult.getNoErrors(), is(0));
        assertThat(thirdResult.getNoSuccesses(), is(1));
        assertThat(thirdResult.getRoamingSubscribers(), is(0));
    }

    private void validateResultForRoamingCountry(final String json) throws Exception {
        final List<RoamingAnalysisResult> roamingResult = getTranslator().translateResult(json,
                RoamingAnalysisResult.class);
        assertThat(roamingResult.size(), is(3));

        final RoamingAnalysisResult firstResult = roamingResult.get(0);
        assertThat(firstResult.getOperator(), is(ARGENTINA));
        assertThat(firstResult.getNoErrors(), is(2));
        assertThat(firstResult.getNoSuccesses(), is(0));
        assertThat(firstResult.getRoamingSubscribers(), is(1));

        final RoamingAnalysisResult secondResult = roamingResult.get(1);
        assertThat(secondResult.getOperator(), is(USA));
        assertThat(secondResult.getNoErrors(), is(1));
        assertThat(secondResult.getNoSuccesses(), is(1));
        assertThat(secondResult.getRoamingSubscribers(), is(1));

        final RoamingAnalysisResult thirdResult = roamingResult.get(2);
        assertThat(thirdResult.getOperator(), is(NORWAY));
        assertThat(thirdResult.getNoErrors(), is(0));
        assertThat(thirdResult.getNoSuccesses(), is(1));
        assertThat(thirdResult.getRoamingSubscribers(), is(0));
    }

    @Override
    protected MultivaluedMap<String, String> getMap() {
        final MultivaluedMap<String, String> map = super.getMap();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        return map;
    }
}
