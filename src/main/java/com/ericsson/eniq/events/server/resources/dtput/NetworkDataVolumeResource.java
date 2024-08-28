/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.resources.BaseResource;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/** 
 * Sub-resource for Data Volume by Network Entity request handling. (Data Throughput feature)
 *
 * @author evarzol
 * @author ehaoswa
 * @since  July 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
@SuppressWarnings("PMD.CyclomaticComplexity")
public class NetworkDataVolumeResource extends BaseResource {

    private static final String NO_TYPE = "NO_TYPE";

    private static final String IS_NODE_TYPE = "isTypeNode";

    @EJB
    private TechPackCXCMappingService techPackCXCMappingService;

    /**
     * Constructor for class
     * Initialising the look up maps - cannot have these as static, or initialised in a static block, as they
     * can be initialised from several classes.
     */
    public NetworkDataVolumeResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(TYPE_APN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));

        aggregationViews.put(TYPE_SGSN, new AggregationTableInfo(TYPE_SGSN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(NO_TYPE, new AggregationTableInfo(GSN_NETWORK, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    private boolean isTypeRequired(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(TYPE_PARAM)) {
            if (!requestParameters.getFirst(TYPE_PARAM).equals(TYPE_APN)
                    && !requestParameters.getFirst(TYPE_PARAM).equals(TYPE_SGSN)) {
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(java.lang.String, javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {

            return getNetworkDataVolumeResults(requestId, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /**
     * Gets the network datavol results.
     *
     * @param requestId the request id
     * @param requestParameters the request parameters
     * @param path the path
     * @return the network datavol results
     */
    public String getNetworkDataVolumeResults(final String requestId,
            final MultivaluedMap<String, String> requestParameters) {

        //If techpack not installed return error
        for (final String techPackName : TechPackData.completeGSNTechPackList) {
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
                requestParameters, TechPackData.completeGSNTechPackList);

        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        String type = requestParameters.getFirst(TYPE_PARAM);
        final String groupName = requestParameters.getFirst(GROUP_NAME_PARAM);
        if (groupName != null) {
            templateParameters.put(GROUP_NAME_PARAM, groupName);
            updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);
        }
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        if (requestParameters.containsKey(TYPE_PARAM)) {
            templateParameters.put(TYPE_PARAM, type);
            templateParameters.put(IS_NODE_TYPE, true);
        } else {
            type = NO_TYPE;
            templateParameters.put(IS_NODE_TYPE, false);
        }
        if (!isTypeRequired(requestParameters)) {
            return JSONUtils.jsonErrorTypeMsg();
        }
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        String template = null;

        template = getTemplate(NETWORK_DATAVOL_ANALYSIS, requestParameters, null);

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, type);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        updateTemplateParameters(requestParameters, templateParameters, timerange, techPackTables);

        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                TechPackData.completeGSNTechPackList);

        final StringBuffer startTime = new StringBuffer("'" + newDateTimeRange.getStartDateTime() + "'");
        templateParameters.put(START_TIME, startTime.toString());

        final String[] dataVolumeDateTimeList = DateTimeRange.getSamplingTimeList(newDateTimeRange,
                DateTimeRange.getChartInterval(newDateTimeRange, timerange));
        final StringBuffer endTime = new StringBuffer("'" + newDateTimeRange.getEndDateTime() + "'");
        templateParameters.put(END_TIME, endTime.toString());

        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, newDateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), null, query, newDateTimeRange);
            return null;
        }

        return getDataVolume(requestId, query, requestParameters, newDateTimeRange, dataVolumeDateTimeList, tzOffset,
                NETWORK_DATA_VOLUME_X_AXIS_VALUE, NETWORK_DATA_VOLUME_SECOND_Y_AXIS_VALUE,
                NETWORK_DATA_VOLUME_X_AXIS_VALUE);
    }

    /**
     * add various template parameters.
     *
     * @param requestParameters the request parameters
     * @param templateParameters the template parameters
     * @param timerange the timerange
     * @param type the type
     * @param techPackTables the tech pack tables
     */
    private void updateTemplateParameters(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters, final String timerange, final TechPackTables techPackTables) {
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(INTERVAL_PARAM, getInterval(timerange));
        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS));
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationTables(timerange));
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
        TechPackTables ret = null;
        if (shouldQueryUseAggregationTables(timerange)) {
            ret = getDTPutAggregationTables(type, timerange);
        } else {
            ret = getDTPutRawTables(type, dateTimeRange);
        }
        return ret;
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

    private int getInterval(final String timerange) {
        if (timerange.equalsIgnoreCase(ONE_MINUTE)) {
            return 1;
        } else if (timerange.equalsIgnoreCase(FIFTEEN_MINUTES)) {
            return 15;
        } else if (timerange.equalsIgnoreCase(DAY)) {
            return 1440;
        } else {
            return 1;
        }
    }

    /**
     * @param requestId corresponds to this request for cancelling later
     * @param query the sql query
     * @param requestParameters the UI request parameters
     * @param newDateTimeRange the formatted input time range
     * @param eventVolumeDateTimeList the event volume time list
     * @param tzOffset the time zone offset
     * @param xAxis the X axis of event volume chart
     * @param secondYAxis the second Y axis of event volume chart
     * @param timeColumn the time column of event volume chart
     * 
     * @return event-volume data
     */
    private String getDataVolume(final String requestId, final String query,
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange newDateTimeRange,
            final String[] dataVolumeDateTimeList, final String tzOffset, final String xAxis, final String secondYAxis,
            final String timeColumn) {
        return this.dataService.getSamplingChartData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), dataVolumeDateTimeList,
                xAxis, secondYAxis, timeColumn, tzOffset, getLoadBalancingPolicy(requestParameters));
    }
}