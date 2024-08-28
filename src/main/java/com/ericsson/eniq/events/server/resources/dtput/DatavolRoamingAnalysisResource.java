/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.dtput;

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
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.resources.BaseResource;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/** 
 * The Class RoamingAnalysisResource.
 *
 * @author evarzol
 * @since  July 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class DatavolRoamingAnalysisResource extends BaseResource {
    @EJB
    private TechPackCXCMappingService techPackCXCMappingService;

    public DatavolRoamingAnalysisResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_NETWORK, new AggregationTableInfo(TYPE_NETWORK, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
    }

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
        final List<String> techPacks = getTechPacksApplicable();

        //If techpack not installed return error
        for (final String techPackName : techPacks) {
            String techPack = techPackName;
            if (techPackName.startsWith("EVENT_E_GSN")) {
                techPack = "EVENT_E_GSN";
            }
            final List<String> cxcLicensesForTechPack = techPackCXCMappingService.getTechPackCXCNumbers(techPack);
            if (cxcLicensesForTechPack.isEmpty()) {
                return JSONUtils.createJSONErrorResult("TechPack " + techPack + " has not been installed.");
            }
        }

        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, techPacks);
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        templateParameters.put(ROAMING_OBJECT, roamingObject);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);

        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                techPacks);

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, TYPE_NETWORK);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationTables(timerange));

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(DATAVOL_ROAMING_ANALYSIS, requestParameters, drillType), templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        return this.dataService.getChartData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), ROAMING_X_AXIS_VALUE,
                ROAMING_SECOND_Y_AXIS_VALUE, null, requestParameters.getFirst(TZ_OFFSET));
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }

    /**
     * Gets the tech pack tables or views.
     *
     * @param dateTimeRange the date time range
     * @param timerange the timerange
     * @param type the type
     * @return the tech pack tables or views
     */
    TechPackTables getTechPackTablesOrViews(final FormattedDateTimeRange dateTimeRange, final String timerange,
            final String type) {
        if (shouldQueryUseAggregationTables(timerange)) {
            return getDTPutAggregationTables(type, timerange, getTechPacksApplicable());
        }
        return getDTPutRawTables(type, dateTimeRange, getTechPacksApplicable());
    }

    /**
     * taken directly from the velocity template.
     *
     * @param timerange the timerange
     * @return true, if should query use aggregation tables
     */
    boolean shouldQueryUseAggregationTables(final String timerange) {
        if (timerange.equals(FIFTEEN_MINUTES) || timerange.equals(DAY)) {
            return true;
        }
        return false;
    }

    /**
     *
     * @param type the type
     * @return the tech packs applicable for type
     */
    private List<String> getTechPacksApplicable() {

        final List<String> listOfApplicableTechPacks = new ArrayList<String>();
        listOfApplicableTechPacks.add(TechPackData.EVENT_E_GSN_DT);
        listOfApplicableTechPacks.add(TechPackData.EVENT_E_GSN_DTPDP);

        return listOfApplicableTechPacks;

    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }
}
