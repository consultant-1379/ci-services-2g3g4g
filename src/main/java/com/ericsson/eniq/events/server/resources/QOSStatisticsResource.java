/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;
import static com.ericsson.eniq.events.server.resources.QOSStatisticsResourceConstants.*;

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
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * Sub-resource for providing QOS Statistics
 * @author eemecoy
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class QOSStatisticsResource extends BaseResource {

    private static List<String> listOfTechPacks = new ArrayList<String>();

    static {
        listOfTechPacks.add(TechPackData.EVENT_E_LTE);
    }

    /**
     * Constructor for class
     * Initializing the look up maps - cannot have these as static, or initialized in a static block, as they
     * can be initialized from several classes
     */
    public QOSStatisticsResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(APN_EVENTID_EVNTSRC_VEND_HIER3,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(VEND_HIER3, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new AggregationTableInfo(VEND_HIER321, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_SGSN, new AggregationTableInfo(APN_EVENTID_EVNTSRC_VEND_HIER3,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_TAC, new AggregationTableInfo(MANUF_TAC, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            return queryUtils.checkValidValue(requestParameters);
        }
        return false;
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
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(GRID_PARAM)) {
            return getGridData(requestId, requestParameters);
        }

        return getNoSuchDisplayErrorResponse(displayType);
    }

    private String getGridData(final String requestId, final MultivaluedMap<String, String> requestParameters) {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, listOfTechPacks);

        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();

        final String timeColumn = null;

        final TechPackTables rawTables = getRawTables(type, dateTimeRange, listOfTechPacks);
        if (rawTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        updateTemplateParametersWithRawAndAggregationTables(requestParameters, templateParameters, type, timerange,
                rawTables);

        addColumnsForQueries(type, templateParameters);
        addColumnsToSelectFromRawTables(type, templateParameters);

        updateTemplateParametersWithTypeAndGroupParams(requestParameters, templateParameters, type);
        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        if (isMediaTypeApplicationCSV()) {
            templateParameters.put(CSV_PARAM, new Boolean(true));
        }

        final FormattedDateTimeRange newDateTimeRange = dateTimeHelper.getDataTieredDateTimeRange(dateTimeRange);

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(QOS_STATISTICS, requestParameters, null, timerange,
                        applicationConfigManager.isDataTieringEnabled()), templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, newDateTimeRange);

        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, null, query, newDateTimeRange);
            return null;
        }
        return this.dataService.getGridData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), timeColumn, tzOffset,
                getLoadBalancingPolicy(requestParameters));

    }

    private void updateTemplateParametersWithRawAndAggregationTables(
            final MultivaluedMap<String, String> requestParameters, final Map<String, Object> templateParameters,
            final String type, final String timerange, final TechPackTables rawTables) {
        templateParameters.put(RAW_TABLES, rawTables);
        final TechPackTables aggregationTables = getAggregationTables(type, timerange, listOfTechPacks);
        templateParameters.put(USE_AGGREGATION_TABLES,
                shouldQueryUseAggregationView(type, timerange, getGroupName(requestParameters)));
        templateParameters.put(AGGREGATION_TABLES, aggregationTables);
    }

    private void updateTemplateParametersWithTypeAndGroupParams(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters, final String type) {
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(GROUP_NAME_PARAM, getGroupName(requestParameters));
    }

    private String getGroupName(final MultivaluedMap<String, String> requestParameters) {
        return requestParameters.getFirst(GROUP_NAME_PARAM);
    }

    /**
     * Build up the list of columns that should be selected from the raw tables - this query doesn't go directly
     * to the raw view, it fetches the applicable list of raw tables instead.
     * Don't want to do a select * on those raw tables for performance reasons so specifying a smaller subset of 
     * columns
     * Could have done this in the VM template, but it doesn't seem to handle list manipulation too well - and
     * still need to specify the type specific columns (which are driven from the java class)
     */
    private void addColumnsToSelectFromRawTables(final String type, final Map<String, Object> templateParameters) {
        final List<String> columnsRequiredFromErrRawTables = new ArrayList<String>();
        columnsRequiredFromErrRawTables.addAll(columnsRequiredFromRawErrTablesForAllQueries);
        columnsRequiredFromErrRawTables.addAll(columnsRequiredFromAllRawTables);
        final List<String> typeSpecificColumns = TechPackData.aggregationColumns.get(type);
        columnsRequiredFromErrRawTables.addAll(typeSpecificColumns);
        templateParameters.put(COLUMNS_TO_SELECT_FROM_RAW_TABLES_FOR_QOS_ERR_SUMMARY, columnsRequiredFromErrRawTables);
        final List<String> columnsRequiredFromSucRawTables = new ArrayList<String>();
        columnsRequiredFromSucRawTables.addAll(columnsRequiredFromRawSucTablesForAllQueries);
        columnsRequiredFromSucRawTables.addAll(columnsRequiredFromAllRawTables);
        columnsRequiredFromSucRawTables.addAll(typeSpecificColumns);
        templateParameters.put(COLUMNS_TO_FILTER_ON, typeSpecificColumns);
        templateParameters.put(COLUMNS_TO_SELECT_FROM_RAW_TABLES_FOR_QOS_SUC_SUMMARY, columnsRequiredFromSucRawTables);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        return checkRequiredParametersExist(requestParameters, TYPE_PARAM);
    }

}
