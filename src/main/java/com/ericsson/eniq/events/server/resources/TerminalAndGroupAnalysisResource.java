/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;

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
import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * The Class TerminalAndGroupAnalysisResource.
 *
 * @author ehaoswa
 * @since  May 2010
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class TerminalAndGroupAnalysisResource extends BaseResource {

    /** The Constant GROUP_TERMINAL. */
    private static final String GROUP_TERMINAL = "GROUP_TERMINAL";

    @EJB
    private TechPackCXCMappingService techPackCXCMapping;

    /** The event ids for failure keys. */
    private static Map<String, Integer[]> eventIdsForFailureKeys = new HashMap<String, Integer[]>();

    static {
        eventIdsForFailureKeys.put(ATTACHED_KEY_VALUE, new Integer[] { ATTACH_IN_2G_AND_3G, ATTACH_IN_4G });
        eventIdsForFailureKeys.put(PDP_KEY_VALUE, new Integer[] { ACTIVATE_IN_2G_AND_3G, PDN_CONNECT_IN_4G });
        eventIdsForFailureKeys.put(MOBILITY_KEY_VALUE, new Integer[] { RAU_IN_2G_AND_3G, ISRAU_IN_2G_AND_3G,
                HANDOVER_IN_4G, TAU_IN_4G });
    }

    /**
     * Constructor for resource, just populates maps required later on.
     */
    public TerminalAndGroupAnalysisResource() {
        //this query doesn't really have a type as such in the traditional sense (like APN, SGSN etc)
        //so using the keys/subpaths to identify the queries
        aggregationViews.put(MOST_ATTACHED_FAILURES, new AggregationTableInfo(MANUF_TAC_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(MOST_PDP_SESSION_SETUP_FAILURES, new AggregationTableInfo(MANUF_TAC_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(MOST_MOBILITY_ISSUES, new AggregationTableInfo(MANUF_TAC_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(MOST_POPULAR_EVENT_SUMMARY, new AggregationTableInfo(MANUF_TAC,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(MOST_POPULAR, new AggregationTableInfo(MANUF_TAC, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(HIGHEST_DATAVOL, new AggregationTableInfo(TERM, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
    @SuppressWarnings("unused")
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the most popular data. 
     *
     * @return the most popular data
     * @throws WebApplicationException the web application exception
     */
    @Path(MOST_POPULAR)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getMostPopularData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, null);

    }

    /**
     * Gets the most popular event summary data.
     *
     * @return the most popular event summary data
     * @throws WebApplicationException the web application exception
     */
    @Path(MOST_POPULAR_EVENT_SUMMARY)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getMostPopularEventSummaryData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, null);

    }

    /**
     * Gets the most attached failures data.
     *
     * @return the most attached failures data
     * @throws WebApplicationException the web application exception
     */
    @Path(MOST_ATTACHED_FAILURES)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getMostAttachedFailuresData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, ATTACHED_KEY_VALUE);

    }

    /**
     * Gets the most pdp session setup failures data.
     *
     * @return the most pdp session setup failures data
     * @throws WebApplicationException the web application exception
     */
    @Path(MOST_PDP_SESSION_SETUP_FAILURES)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getMostPDPSessionSetupFailuresData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, PDP_KEY_VALUE);

    }

    /**
     * Gets the most mobility issues data.
     *
     * @return the most mobility issues data
     * @throws WebApplicationException the web application exception
     */
    @Path(MOST_MOBILITY_ISSUES)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getMostMobilityIssuesData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, MOBILITY_KEY_VALUE);

    }

    /**
     * Gets the highest data volume data ranked by total data volume.
     *
     * @return the most mobility issues data
     * @throws WebApplicationException the web application exception
     */
    @Path(HIGHEST_DATAVOL)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getHighestDatavolData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, null);
    }

    /**
     * Gets the terminal and group analysis results.
     *
     * @param requestId the request id
     * @param requestParameters the request parameters
     * @param failureKey the failure key
     * @param uriPath the uri path
     * @return the terminal and group analysis results
     * @throws WebApplicationException the web application exception
     */
    protected String getTerminalAndGroupAnalysisResults(final String requestId,
            final MultivaluedMap<String, String> requestParameters, final String failureKey, final String uriPath)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getTerminalAndGroupAnalysisDataResults(requestId, requestParameters, failureKey, uriPath);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    public void setTechPackCXCMapping(final TechPackCXCMappingService techPackCXCMapping) {
        this.techPackCXCMapping = techPackCXCMapping;
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
     * @param requestId the request id
     * @param requestParameters - URL query parameters
     * @param failureKey the failure key
     * @param uriPath the URI path
     * @return JSON encoded results
     * @throws WebApplicationException the web application exception
     */
    private String getTerminalAndGroupAnalysisDataResults(final String requestId,
            final MultivaluedMap<String, String> requestParameters, final String failureKey, final String uriPath)
            throws WebApplicationException {

        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        addCountParamIfRequired(requestParameters, uriPath, templateParameters);

        addEventIdsForFailureKeyIfNotBlank(failureKey, templateParameters);

        final String subPath = getSubPathFromURI(uriPath);

        List<String> techPackList;
        if (!subPath.equals(HIGHEST_DATAVOL)) {
            techPackList = TechPackData.completeSGEHTechPackList;
        } else {
            techPackList = TechPackData.completeGSNTechPackList;
        }

        //If techpack not installed return error
        for (String techPackName : techPackList) {
            if (techPackName.equals(TechPackData.EVENT_E_GSN_DT) || techPackName.equals(TechPackData.EVENT_E_GSN_DTPDP)) {
                techPackName = "EVENT_E_GSN";
            }
            final List<String> cxcLicensesForTechPack = techPackCXCMapping.getTechPackCXCNumbers(techPackName);
            if (cxcLicensesForTechPack.isEmpty()) {
                return JSONUtils.createJSONErrorResult(techPackName + " has not been installed.");
            }
        }

        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, techPackList);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);

        FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                techPackList);
        final String groupName = requestParameters.getFirst(GROUP_NAME_PARAM);
        if (!subPath.equals(HIGHEST_DATAVOL)) {
            populateTableParameters(subPath, templateParameters, newDateTimeRange, timerange, groupName);
        } else {
            populateDTPutTableParameters(subPath, templateParameters, newDateTimeRange, timerange);
        }
        if (shouldReportErrorAboutEmptyTables(templateParameters)) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        final StringBuffer sb = new StringBuffer();
        String pathNameInTemplateMap = null;
        if (uriPath.contains(TERMINAL_GROUP_ANALYSIS)) {
            pathNameInTemplateMap = sb.append(TERMINAL_GROUP_ANALYSIS).append("/").append(subPath).toString();
        } else if (uriPath.contains(TERMINAL_ANALYSIS)) {
            pathNameInTemplateMap = sb.append(TERMINAL_ANALYSIS).append("/").append(subPath).toString();
        }

        String template;
        if (subPath.equals(MOST_POPULAR_EVENT_SUMMARY)) {
            newDateTimeRange = dateTimeHelper.getDataTieredDateTimeRange(newDateTimeRange);
            template = getTemplate(pathNameInTemplateMap, requestParameters, null, timerange,
                    applicationConfigManager.isDataTieringEnabled());
        } else {
            template = getTemplate(pathNameInTemplateMap, requestParameters, null);
        }

        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }
        return getDataForDisplayType(requestId, requestParameters, uriPath, tzOffset, newDateTimeRange, query);
    }

    private boolean shouldReportErrorAboutEmptyTables(final Map<String, Object> templateParameters) {
        final TechPackTables rawTables = getRawTablesConfigured(templateParameters);
        return rawTables.shouldReportErrorAboutEmptyRawTables();
    }

    /**
     * Returns the templateParameter for RAW_TABLES if isn't null - otherwise, it returns the
     * templateParmeter for RAW_DTPUT_TABLES
     * 
     * @param templateParameters
     * @return TechPackTables
     */
    private TechPackTables getRawTablesConfigured(final Map<String, Object> templateParameters) {
        TechPackTables rawTables = (TechPackTables) templateParameters.get(RAW_TABLES);
        if (rawTables == null) {
            rawTables = (TechPackTables) templateParameters.get(RAW_DTPUT_TABLES);
        }
        return rawTables;
    }

    /**
     * Gets the sub path from uri.
     *
     * @param uriPath the uri path
     * @return the sub path from uri
     */
    private String getSubPathFromURI(final String uriPath) {
        return uriPath.substring(uriPath.lastIndexOf(PATH_SEPARATOR) + 1, uriPath.length());
    }

    /**
     * Populate table parameters.
     *
     * @param subPath the sub path
     * @param templateParameters the template parameters
     * @param newDateTimeRange the new date time range
     * @param timerange the timerange
     */
    private void populateTableParameters(final String subPath, final Map<String, Object> templateParameters,
            final FormattedDateTimeRange newDateTimeRange, final String timerange, final String groupName) {
        final TechPackTables aggregationTables = getAggregationTables(subPath, timerange);
        templateParameters.put(AGGREGATION_TABLES, aggregationTables);
        final TechPackTables rawTables = getRawTables(GROUP_TERMINAL, newDateTimeRange);
        templateParameters.put(RAW_TABLES, rawTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationView(subPath, timerange, groupName));
        templateParameters.put(GROUP_NAME_PARAM, groupName);
    }

    /**
     * Populate dtput table parameters.
     *
     * @param subPath the sub path
     * @param templateParameters the template parameters
     * @param newDateTimeRange the new date time range
     * @param timerange the timerange
     */
    private void populateDTPutTableParameters(final String subPath, final Map<String, Object> templateParameters,
            final FormattedDateTimeRange newDateTimeRange, final String timerange) {
        final TechPackTables dTPutAggregationTables = getDTPutAggregationTables(subPath, timerange);
        templateParameters.put(DTPUT_AGGREGATION_TABLES, dTPutAggregationTables);
        final TechPackTables dtputRawTables = getDTPutRawTables(GROUP_TERMINAL, newDateTimeRange);
        templateParameters.put(RAW_DTPUT_TABLES, dtputRawTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationView(subPath, timerange));
    }

    /**
     * Gets the data for display type.
     *
     * @param requestId the request id
     * @param requestParameters the request parameters
     * @param uriPath the uri path
     * @param tzOffset the tz offset
     * @param newDateTimeRange the new date time range
     * @param query the query
     * @return the data for display type
     */
    private String getDataForDisplayType(final String requestId,
            final MultivaluedMap<String, String> requestParameters, final String uriPath, final String tzOffset,
            final FormattedDateTimeRange newDateTimeRange, final String query) {
        String results;
        if (requestParameters.getFirst(DISPLAY_PARAM).equals(CHART_PARAM)) {
            if (uriPath.contains(MOST_POPULAR_EVENT_SUMMARY)) {
                results = this.dataService.getGroupsMostFreqSignalChartData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), null, tzOffset);
            } else {
                results = this.dataService.getChartData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange),
                        TERMINAL_GROUP_ANALYSIS_X_AXIS_VALUE, getSecondYAxis(uriPath), null, tzOffset);
            }
        } else {
            if (isMediaTypeApplicationCSV()) {
                streamDataAsCSV(requestParameters, tzOffset, null, query, newDateTimeRange);
                results = null;

            } else {
                results = this.dataService.getGridData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), null, tzOffset);
            }
        }
        return results;
    }

    /**
     * Adds the event ids for failure key if not blank.
     *
     * @param failureKey the failure key
     * @param templateParameters the template parameters
     */
    private void addEventIdsForFailureKeyIfNotBlank(final String failureKey,
            final Map<String, Object> templateParameters) {
        if (StringUtils.isNotBlank(failureKey)) {
            final Integer[] eventIdsForKey = eventIdsForFailureKeys.get(failureKey);
            final String sqlArrayOfIntegers = formatIntegersIntoSQLArray(eventIdsForKey);
            templateParameters.put(EVENT_IDS, sqlArrayOfIntegers);
        }
    }

    /**
     * Format integers into sql array.
     *
     * @param eventIdsForKey the event ids for key
     * @return the string
     */
    private String formatIntegersIntoSQLArray(final Integer[] eventIdsForKey) {
        final StringBuilder sb = new StringBuilder(OPENING_BRACKET);
        for (final int eventID : eventIdsForKey) {
            sb.append(eventID);
            sb.append(COMMA);
        }
        deleteLastComma(sb);
        sb.append(CLOSING_BRACKET);
        return sb.toString();
    }

    /**
     * Delete last comma.
     *
     * @param sb the sb
     */
    private void deleteLastComma(final StringBuilder sb) {
        sb.deleteCharAt(sb.length() - 1);
    }

    /**
     * Adds the count param if required.
     *
     * @param requestParameters the request parameters
     * @param uriPath the uri path
     * @param templateParameters the template parameters
     */
    private void addCountParamIfRequired(final MultivaluedMap<String, String> requestParameters, final String uriPath,
            final Map<String, Object> templateParameters) {
        if ((uriPath.contains(TERMINAL_GROUP_ANALYSIS) && !requestParameters.containsKey(GROUP_NAME_PARAM))
                || uriPath.contains(TERMINAL_ANALYSIS)) {
            templateParameters
                    .put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS));
        }
    }

    /**
     * Gets the second y axis.
     *
     * @param uriPath the uri path
     * @return the second y axis
     */
    private String getSecondYAxis(final String uriPath) {
        String secondYaxis = null;
        if (uriPath.contains(MOST_POPULAR_EVENT_SUMMARY)) {
            secondYaxis = TERMINAL_GROUP_ANALYSIS_MOST_EVENT_SUMMARY_SECOND_Y_AXIS_VALUE;
        } else if (uriPath.contains(HIGHEST_DATAVOL)) {
            secondYaxis = TERMINAL_GROUP_ANALYSIS_HIGHEST_DATAVOL_SECOND_Y_AXIS_VALUE;
        }
        return secondYaxis;
    }

    /**
     * Gets the request data.
     *
     * @param requestId the request id
     * @param failureKey the failure key
     * @return the request data
     */
    private String getRequestData(final String requestId, final String failureKey) {
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        return getTerminalAndGroupAnalysisResults(requestId, requestParameters, failureKey, uriInfo.getAbsolutePath()
                .getPath());
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @SuppressWarnings("unused")
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

}
