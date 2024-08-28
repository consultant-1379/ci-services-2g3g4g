/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;
import com.ericsson.eniq.events.server.utils.techpacks.ViewTypeSelector;

/**
 * The Class RoamingAnalysisResource.
 *
 * @author ehaoswa
 * @since  May 2010
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class RoamingAnalysisResource extends BaseResource {

    @EJB
    protected RoamingDrillByCountryResource roamingDrillByCountryResource;

    @EJB
    protected RoamingDrillByOperatorResource roamingDrillByOperatorResource;

    @EJB
    protected RoamingDrillByCountryDetailResource roamingDrillByCountrylDetailResource;

    @EJB
    protected RoamingDrillByOperatorDetailResource roamingDrillByOperatorDetailResource;

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    @Path(ROAMING_COUNTRY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRoamingCountryData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRoamingResults(requestId, TYPE_ROAMING_COUNTRY, getDecodedQueryParameters());
    }

    /**
     * roaming by country ,show number of failures and impacted subscribers by event id
     * @return
     */

    @Path(ROAMING_COUNTRY_DRILL)
    @Produces(MediaType.APPLICATION_JSON)
    public RoamingDrillByCountryResource getRoamingCountryDrillResource() {
        return this.roamingDrillByCountryResource;
    }

    /**
     * roaming by country , show detail rows for that particualr event id
     * @return
     */
    @Path(ROAMING_COUNTRY_DRILL_DETAIL)
    @Produces(MediaType.APPLICATION_JSON)
    public RoamingDrillByCountryDetailResource getRoamingDrillByCountrylDetailResource() {
        return this.roamingDrillByCountrylDetailResource;
    }

    /**
     * Roaming by operator.
     * 
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    @Path(ROAMING_OPERATOR)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRoamingOperatorData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRoamingResults(requestId, TYPE_ROAMING_OPERATOR, getDecodedQueryParameters());
    }

    /**
     * roaming by operator return number of failures and impacted subscribers by each eventID
     *
     * @return
     */

    @Path(ROAMING_OPERATOR_DRILL)
    @Produces(MediaType.APPLICATION_JSON)
    public RoamingDrillByOperatorResource getRoamingOperatorDrillResource() {
        return this.roamingDrillByOperatorResource;
    }

    /**
     * roaming by operator ,drill to detail grid for the given event id
     * @return
     */

    @Path(ROAMING_OPERATOR_DRILL_DETAIL)
    @Produces(MediaType.APPLICATION_JSON)
    public RoamingDrillByOperatorDetailResource getRoamingDrillByOperatorDetailResource() {
        return this.roamingDrillByOperatorDetailResource;
    }

    /**
     * Get Roaming Results:
     * (Both grid and chart display types as user can for example 
     * change time when in grid version of chart - so valid that "grid" can be in call
     * the UI can handle chart style data in the grid).
     * 
     * @param requestId corresponds to this request for cancelling later
     * @param roamingObject - roaming object [county,operator]
     * @param requestParameters - URL query parameters
     * @return JSON encoded string
     * @throws WebApplicationException
     */
    public String getRoamingResults(final String requestId, final String roamingObject,
            final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {

        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getChartResults(requestId, roamingObject, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * Gets the JSON chart results for network roaming.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param dateTimeRange - formatted date time range
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    private String getChartResults(final String requestId, final String roamingObject,
            final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        final String drillType = null;
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, TechPackData.completeSGEHTechPackList);
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        templateParameters.put(ROAMING_OBJECT, roamingObject);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);

        FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                TechPackData.completeSGEHTechPackList);

        if (applicationConfigManager.isDataTieringEnabled()) {
            newDateTimeRange = dateTimeHelper.getDataTieredDateTimeRange(newDateTimeRange);
            templateParameters.put(SUC_TIMERANGE,
                    ViewTypeSelector.returnSuccessAggregateViewType(
                            EventDataSourceType.getEventDataSourceType(timerange),
                            TechPackData.completeSGEHTechPackList.get(0)));
        }

        final String type = requestParameters.getFirst(TYPE_PARAM);
        templateParameters.put(TECH_PACK_TABLES,
                getRawTables(type, dateTimeRange, TechPackData.completeSGEHTechPackList));

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(ROAMING_ANALYSIS, requestParameters, drillType, timerange,
                        applicationConfigManager.isDataTieringEnabled()), templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        return this.dataService.getChartData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), ROAMING_X_AXIS_VALUE,
                ROAMING_SECOND_Y_AXIS_VALUE, null, requestParameters.getFirst(TZ_OFFSET));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }
}
