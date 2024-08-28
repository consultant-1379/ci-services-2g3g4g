/**
 * -----------------------------------------------------------------------
jsonAssertUtils *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.test.util.JSONAssertUtils;
import com.ericsson.eniq.events.server.utils.QueryUtils;
import com.ericsson.eniq.events.server.utils.parameterchecking.ParameterChecker;
import com.ericsson.eniq.events.server.utils.techpacks.TechPackListFactory;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 *
 */
public class QOSStatisticsResourceTest {

    private static final List<String> LIST_OF_APPLICABLE_TECHPACKS = new ArrayList<String>();

    private QOSStatisticsResource qosStatisticsResource;

    private JSONAssertUtils jsonAssertUtils;

    @Before
    public void setup() {
        qosStatisticsResource = new QOSStatisticsResource();
        final QueryUtils queryUtils = new QueryUtils();
        queryUtils.setParameterChecker(new ParameterChecker());
        qosStatisticsResource.setQueryUtils(queryUtils);
        qosStatisticsResource.setTechPackListFactory(new TechPackListFactory());
        LIST_OF_APPLICABLE_TECHPACKS.add(EVENT_E_LTE);
        jsonAssertUtils = new JSONAssertUtils();
    }

    @Test
    public void testAddViewNamesToTemplateParameters_APN_DAYAggregation() {
        final TechPackTables techPackTables = qosStatisticsResource.getTechPackTablesOrViews(null,
                EventDataSourceType.AGGREGATED_DAY.toString(), TYPE_APN, LIST_OF_APPLICABLE_TECHPACKS);
        assertThat(techPackTables.getErrTables().get(0), is("EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY"));
    }

    @Test
    public void testIsValidValueWithValidNodeParam() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TYPE_PARAM, TYPE_APN);
        requestParameters.add(NODE_PARAM, "someAPn");
        assertThat(qosStatisticsResource.isValidValue(requestParameters), is(true));
    }

    @Test
    public void testGetDataWithMissingNodeAndGroupParameter() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, APN_PARAM);
        final String result = qosStatisticsResource.getData(null, requestParameters);
        jsonAssertUtils.assertJSONErrorResult(result);
        jsonAssertUtils.assertResultContains(result, E_INVALID_VALUES);
    }

    @Test
    public void testGetDataWithMissingTYPEParameter() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(NODE_PARAM, "some node");
        final String result = qosStatisticsResource.getData(null, requestParameters);
        jsonAssertUtils.assertJSONErrorResult(result);
        jsonAssertUtils.assertResultContains(result, E_INVALID_OR_MISSING_PARAMS);
    }

}
