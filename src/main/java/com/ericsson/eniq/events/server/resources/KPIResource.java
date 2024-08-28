/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;

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
import com.ericsson.eniq.events.server.kpi.KPIQueryfactory;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;
import com.ericsson.eniq.events.server.utils.techpacks.ViewTypeSelector;

/**
 * @author ehaoswa
 * @since May 2010
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
@SuppressWarnings("PMD.CyclomaticComplexity")
//NOPMD ehaoswa 29/07/2010
public class KPIResource extends BaseResource {

    @EJB
    protected KPIQueryfactory lteQueryBuilder;

    @Override
    public String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {

        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getChartResults(requestId, getDecodedQueryParameters());
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(TYPE_PARAM)) {
            errors.add(TYPE_PARAM);
        }
        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * Gets the JSON chart results for KPI.
     * For Single KPI, service is sampling 30 results based on timerange and interval
     *
     * @param requestId         corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param dateTimeRange     - formatted date time range
     *
     * @return JSON encoded results
     *
     * @throws WebApplicationException
     */
    private String getChartResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final String drillType = null;
        final List<String> techPackList = new ArrayList<String>();
        techPackList.add(EVENT_E_SGEH);
        techPackList.add(EVENT_E_LTE);
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, techPackList);
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        templateParameters.put(TYPE_PARAM, type);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);

        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        final FormattedDateTimeRange newDateTimeRange = dateTimeHelper
                .getDataTieredDateTimeRange(getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                        techPackList));

        final StringBuffer startTime = new StringBuffer("'" + newDateTimeRange.getStartDateTime() + "'");
        templateParameters.put(START_TIME, startTime.toString());

        final String[] kpiDateTimeList = DateTimeRange.getSamplingTimeList(newDateTimeRange,
                DateTimeRange.getChartInterval(newDateTimeRange, timerange));
        final StringBuffer endTime = new StringBuffer("'" + newDateTimeRange.getEndDateTime() + "'");
        templateParameters.put(END_TIME, endTime.toString());

        updateTemplateWithRAWTables(templateParameters, newDateTimeRange);
        addExclusiveTacRelatedBooleanToTemplateParams(requestParameters, templateParameters);

        final List<String> queries = new ArrayList<String>();

        if (applicationConfigManager.isDataTieringEnabled()) {
            templateParameters.put(
                    SUC_TIMERANGE,
                    ViewTypeSelector.returnSuccessAggregateViewType(
                            EventDataSourceType.getEventDataSourceType(timerange), techPackList.get(0)));
        }

        final String query2G = templateUtils.getQueryFromTemplate(
                getTemplate(KPI, requestParameters, drillType, timerange,
                        applicationConfigManager.isDataTieringEnabled()), templateParameters);

        if (StringUtils.isBlank(query2G)) {
            return JSONUtils.JSONBuildFailureError();
        }
        queries.add(query2G);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query2G, newDateTimeRange);

        if (requestParameters.get(GROUP_NAME_PARAM) != null) {
            templateParameters.put(GROUP_NAME_PARAM, requestParameters.get(GROUP_NAME_PARAM));
        }
        final String query4G = lteQueryBuilder.getLteKPIQuery(templateParameters,
                applicationConfigManager.isDataTieringEnabled());

        if (StringUtils.isBlank(query4G)) {
            return JSONUtils.JSONBuildFailureError();
        }
        queries.add(query4G);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query4G, newDateTimeRange);

        return this.dataService.getSamplingChartData(requestId, queries,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), kpiDateTimeList,
                KPI_X_AXIS_VALUE, KPI_SECOND_Y_AXIS_VALUE, getEventTimeColumnIndex(newDateTimeRange), tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

    /**
     * In the case of a day aggregation, we shouldn't apply TZ offsets to the resultset data
     *
     * @param newDateTimeRange
     *
     * @return
     */
    private String getEventTimeColumnIndex(final FormattedDateTimeRange timerange) {
        if (queryUtils.getEventDataSourceType(timerange).equals(EventDataSourceType.AGGREGATED_DAY)) {
            return null;
        }
        return EVENT_TIME_COLUMN_INDEX;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)
                || requestParameters.containsKey(IMSI_PARAM) || requestParameters.containsKey(PTMSI_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    private void addExclusiveTacRelatedBooleanToTemplateParams(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {

        boolean useTacExclude = true;
        if (requestParameters.containsKey(NODE_PARAM) && requestParameters.getFirst(TYPE_PARAM).equals(TAC)) {
            useTacExclude = false;
        } else {
            String groupName = null;
            String tacParam = null;
            if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
                groupName = requestParameters.getFirst(GROUP_NAME_PARAM);
            }

            if (requestParameters.containsKey(TAC_PARAM)) {
                tacParam = requestParameters.getFirst(TAC_PARAM);
            }
            useTacExclude = !queryIsExclusiveTacRelated(groupName, tacParam);
        }
        templateParameters.put(USE_TAC_EXCLUSION_BOOLEAN, useTacExclude);
    }

    /**
     * This function is used for updating the template parameters with the raw table names
     * which could replace a raw view in the template (SQL query) for a particular time range.
     *
     * @param templateParameters the parameters used inside the query template
     * @param dateTimeRange      the date and time range for the query
     */

    void updateTemplateWithRAWTables(final Map<String, Object> templateParameters,
            final FormattedDateTimeRange dateTimeRange) {

        if (!updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_ERR, RAW_ERR_TABLES)) {
            add4GWorkaround(DC_EVENT_E_SGEH_ERR_RAW_01, RAW_ERR_TABLES, templateParameters);
        }
        if (!updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_SUC, RAW_SUC_TABLES)) {
            add4GWorkaround(DC_EVENT_E_SGEH_SUC_RAW_01, RAW_SUC_TABLES, templateParameters);
        }

        if (!updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_ERR, RAW_LTE_ERR_TABLES)) {
            add4GWorkaround(DC_EVENT_E_LTE_ERR_RAW_01, RAW_LTE_ERR_TABLES, templateParameters);
        }
        if (!updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_SUC, RAW_LTE_SUC_TABLES)) {
            add4GWorkaround(DC_EVENT_E_LTE_SUC_RAW_01, RAW_LTE_SUC_TABLES, templateParameters);
        }
    }

    /**
     * Method specifically added to counter the fact that in a 4g only deployment, there is no 2g4g data and so the
     * get table names methods will fail, and we need to add some table name so that the KPIs will run. This is a dirty
     * hack, but there is very little we can do because the UI expects that all KPIs were run and there is no way
     * of turning on or off kpis.
     *
     * @param templateParameters
     */
    private void add4GWorkaround(final String tableName, final String key, final Map<String, Object> templateParameters) {
        //Tempory fix, to make sure that if we can't get a tablename that we use the view instead
        final List<String> tableNames = new ArrayList<String>();
        tableNames.add(tableName);
        templateParameters.put(key, tableNames);
    }

    public void setLteQueryBuilder(final KPIQueryfactory lteQueryBuilder) {
        this.lteQueryBuilder = lteQueryBuilder;
    }

    /*
     * These four table names are for the 4g workaround. They are used as default values when we can't get the
     * table names from any other source. They are purely used because the UI expects that there is always the same
     * number of KPIs in the JSON, which means we can't not run kpi queries.
     * I have left these in this class because when the workaround is fixed these should be removed and it will be
     * easier to find them here.
     */
    private static final String DC_EVENT_E_SGEH_ERR_RAW_01 = "dc.EVENT_E_SGEH_ERR_RAW_01";

    private static final String DC_EVENT_E_SGEH_SUC_RAW_01 = "dc.EVENT_E_SGEH_SUC_RAW_01";

    private static final String DC_EVENT_E_LTE_ERR_RAW_01 = "dc.EVENT_E_LTE_ERR_RAW_01";

    private static final String DC_EVENT_E_LTE_SUC_RAW_01 = "dc.EVENT_E_LTE_SUC_RAW_01";

}