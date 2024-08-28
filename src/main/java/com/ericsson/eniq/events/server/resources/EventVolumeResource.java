/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.resources.EventVolumeResourceConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * The Class EventVolumeResource.
 * This service returns event volume data for the UI widgets from Sybase IQ. 
 * The actual data comes from a normal JSON data source. 
 * 
 * @author eavidat
 * @author ehaoswa
 * @since May 2010 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
@SuppressWarnings("PMD.CyclomaticComplexity")
public class EventVolumeResource extends BaseResource {

    private static List<String> listOfTechPacks = new ArrayList<String>();

    static {
        listOfTechPacks.add(EVENT_E_SGEH);
        listOfTechPacks.add(EVENT_E_LTE);
    }

    public EventVolumeResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(APN_EVENTID, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(VEND_HIER3_EVENTID,
                EventDataSourceType.AGGREGATED_1MIN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new AggregationTableInfo(VEND_HIER321_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_SGSN, new AggregationTableInfo(EVNTSRC_EVENTID, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(NO_TYPE, new AggregationTableInfo(EVNTSRC_EVENTID, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {

            return getChartResults(requestId, requestParameters);
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

    /**
     * Gets the chart results.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @return JSON encoded results
     * @throws WebApplicationException the parse exception
     */
    private String getChartResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final String drillType = null;
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, listOfTechPacks);
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        String type = requestParameters.getFirst(TYPE_PARAM);
        if (requestParameters.containsKey(TYPE_PARAM)) {
            templateParameters.put(TYPE_PARAM, type);
            templateParameters.put(IS_NODE_TYPE, true);
        } else {
            type = NO_TYPE;
            templateParameters.put(IS_NODE_TYPE, false);
        }

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(INTERVAL_PARAM, getInterval(timerange));

        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);
        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, queryUtils
                .getEventDataSourceType(dateTimeRange).getValue(), type, listOfTechPacks);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        templateParameters.put(TECH_PACK_TABLES, techPackTables);

        templateParameters.put(SUC_AGGREGATION_TABLES, getAggregationTables(type, timerange, listOfTechPacks)
                .getSucTables());

        if (shouldQueryUseAggregationView(type, queryUtils.getEventDataSourceType(dateTimeRange).getValue())) {
            templateParameters.put(USE_AGGREGATION_TABLES, true);
            final TechPackTables rawAllErrTables = getRawTables(type, dateTimeRange);
            if (rawAllErrTables.shouldReportErrorAboutEmptyRawTables()) {
                return JSONUtils.JSONEmptySuccessResult();
            }
            templateParameters.put(IMSI_COUNT_TABLES, rawAllErrTables.getErrTables());
        } else {
            templateParameters.put(IMSI_COUNT_TABLES, techPackTables.getErrTables());
        }

        templateParameters.put(RAW_COLUMNS, columnsToIncludeInRAWView.get(type));
        templateParameters.put(IMSI_COLUMNS, columnsToIncludeInIMSICount.get(type));
        templateParameters.put(AGG_ERR_COLUMNS, columnsToIncludeInAggViewErr.get(type));
        templateParameters.put(AGG_SUC_COLUMNS, columnsToIncludeInAggViewSuc.get(type));
        templateParameters.put(COLUMNS, aggregationColumns.get(type));

        FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                listOfTechPacks);

        if (applicationConfigManager.isDataTieringEnabled()) {
            newDateTimeRange = dateTimeHelper.getDataTieredDateTimeRange(newDateTimeRange);
        }

        final StringBuffer startTime = new StringBuffer("'" + newDateTimeRange.getStartDateTime() + "'");
        templateParameters.put(START_TIME, startTime.toString());

        final String[] eventVolumeDateTimeList = DateTimeRange.getSamplingTimeList(newDateTimeRange,
                DateTimeRange.getChartInterval(newDateTimeRange, timerange));
        final StringBuffer endTime = new StringBuffer("'" + newDateTimeRange.getEndDateTime() + "'");
        templateParameters.put(END_TIME, endTime.toString());
        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(EVENT_VOLUME, requestParameters, drillType, timerange,
                        applicationConfigManager.isDataTieringEnabled()), templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        if (requestParameters.containsKey(TYPE_PARAM)) {
            return getEventVolume(requestId, query, requestParameters, newDateTimeRange, eventVolumeDateTimeList,
                    tzOffset, EVENT_VOLUME_X_AXIS_VALUE, EVENT_VOLUME_SECOND_Y_AXIS_VALUE, EVENT_VOLUME_X_AXIS_VALUE);
        }

        return getEventVolume(requestId, query, requestParameters, newDateTimeRange, eventVolumeDateTimeList, tzOffset,
                NETWORK_EVENT_VOLUME_X_AXIS_VALUE, null, NETWORK_EVENT_VOLUME_X_AXIS_VALUE);
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
    private String getEventVolume(final String requestId, final String query,
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange newDateTimeRange,
            final String[] eventVolumeDateTimeList, final String tzOffset, final String xAxis,
            final String secondYAxis, final String timeColumn) {
        return this.dataService.getSamplingChartData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), eventVolumeDateTimeList,
                xAxis, secondYAxis, timeColumn, tzOffset, getLoadBalancingPolicy(requestParameters));
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
}
