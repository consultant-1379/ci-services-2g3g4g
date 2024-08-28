/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
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
import com.ericsson.eniq.events.server.common.Group;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.resources.BaseResource;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * The Class DatavolRankingResource.
 *
 * @author ehaoswa
 * @since 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class DatavolRankingResource extends BaseResource {
    @EJB
    private TechPackCXCMappingService techPackCXCMappingService;

    /**
     * Constructor for class
     * Initializing the look up maps - cannot have these as static, or initialized in a static block, as they
     * can be initialized from several classes.
     */
    public DatavolRankingResource() {
        typesRestrictedToOneTechPack = new HashMap<String, String>();
        typesRestrictedToOneTechPack.put(TYPE_TAC, TechPackData.EVENT_E_GSN_DT);
        typesRestrictedToOneTechPack.put(TYPE_IMSI, TechPackData.EVENT_E_GSN_DT);
        typesRestrictedToOneTechPack.put(TYPE_APN, TechPackData.EVENT_E_GSN_DT);
        typesRestrictedToOneTechPack.put(TYPE_SGSN, TechPackData.EVENT_E_GSN_DT);
        typesRestrictedToOneTechPack.put(TYPE_GGSN, TechPackData.EVENT_E_GSN_DT);
        typesRestrictedToOneTechPack.put(TYPE_QOS, TechPackData.EVENT_E_GSN_DT);

        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(TYPE_APN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_TAC, new AggregationTableInfo(TERM, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));

        aggregationViews.put(TYPE_SGSN, new AggregationTableInfo(TYPE_SGSN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_GGSN, new AggregationTableInfo(TYPE_GGSN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_QOS, new AggregationTableInfo(TYPE_QOS, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
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
        if (displayType.equals(GRID_PARAM)) {

            return getDatavolRankingResults(requestId, requestParameters, uriInfo.getPath());
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /**
     * Gets the datavol ranking results.
     *
     * @param requestId the request id
     * @param requestParameters the request parameters
     * @param path the path
     * @return the datavol ranking results
     */
    protected String getDatavolRankingResults(final String requestId,
            final MultivaluedMap<String, String> requestParameters, final String path) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final List<String> techPacks = getTechPacksApplicableForType(type);

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

        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();

        final String timeColumn = null;
        String template = null;
        if (path.contains(DATAVOL_GROUP_RANKING_ANALYSIS)) {
            aggregationViews.put(TYPE_IMSI, new AggregationTableInfo(IMSI_GROUP_RANK,
                    EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
            final Map<String, Group> templateGroupDefs = dataService.getGroupsForTemplates();
            templateParameters.put(GROUP_DEFINITIONS, templateGroupDefs);
            template = getTemplate(DATAVOL_GROUP_RANKING_ANALYSIS, requestParameters, null);
        } else {
            aggregationViews.put(TYPE_IMSI, new AggregationTableInfo(IMSI_RANK, EventDataSourceType.AGGREGATED_15MIN,
                    EventDataSourceType.AGGREGATED_DAY));
            template = getTemplate(DATAVOL_RANKING_ANALYSIS, requestParameters, null);
        }

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, type);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        updateTemplateParameters(requestParameters, templateParameters, timerange, type, techPackTables);

        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        final FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                techPacks);

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, newDateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), null, query, newDateTimeRange);
            return null;
        }

        Map<String, QueryParameter> queryParameters;
        if (type.equals(TYPE_APN)) {
            queryParameters = mapAPNRequestParameters(requestParameters, newDateTimeRange);
        } else {
            queryParameters = this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange);
        }

        return this.dataService.getGridData(requestId, query, queryParameters, timeColumn,
                requestParameters.getFirst(TZ_OFFSET), getLoadBalancingPolicy(requestParameters));
    }

    /**
     * maps the required parameters for queries, overridden since specific action is required for APN Ranking
     * @param requestParameters
     * @param newDateTimeRange
     * @return
     */
    @Override
    protected Map<String, QueryParameter> mapQueryParameters(final MultivaluedMap<String, String> requestParameters,
            final FormattedDateTimeRange newDateTimeRange) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        Map<String, QueryParameter> queryParameters;
        if (type.equals(TYPE_APN)) {
            queryParameters = mapAPNRequestParameters(requestParameters, newDateTimeRange);
        } else {
            queryParameters = this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange);
        }
        return queryParameters;
    }

    /**
     * map the various parameters required for APN ranking.
     *
     * @param requestParameters the request parameters
     * @param newDateTimeRange the new date time range
     * @return the map< string, query parameter>
     */
    private Map<String, QueryParameter> mapAPNRequestParameters(final MultivaluedMap<String, String> requestParameters,
            final FormattedDateTimeRange newDateTimeRange) {
        Map<String, QueryParameter> queryParameters;
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final String apnRetentionInDays = Integer.toString(applicationConfigManager.getAPNRetention());
        final FormattedDateTimeRange dateTimeRangeForApn = DateTimeRange.getFormattedTimeRangeInDays(tzOffset,
                apnRetentionInDays);
        queryParameters = this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange);
        queryParameters = this.queryUtils.addDateParametersForApnRetention(queryParameters, dateTimeRangeForApn);
        return queryParameters;
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
            final Map<String, Object> templateParameters, final String timerange, final String type,
            final TechPackTables techPackTables) {
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(TIMERANGE_PARAM, timerange);
        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS));

        templateParameters.put(TECH_PACK_TABLES, techPackTables);

        addColumnsForQueries(requestParameters.getFirst(TYPE_PARAM), templateParameters);
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
        if (shouldQueryUseAggregationTables(timerange)) {
            return getDTPutAggregationTables(type, timerange, getTechPacksApplicableForType(type));
        }
        return getDTPutRawTables(type, dateTimeRange, getTechPacksApplicableForType(type));
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
     * If a particular type is restricted to only EVENT_E_GSN_DT tech pack (i.e. There is only one techpack is used here)
     * then this method returns only that tech pack(s)
     * Otherwise just returns a empty list
     *
     * @param type the type
     * @return the tech packs applicable for type
     */
    private List<String> getTechPacksApplicableForType(final String type) {

        final List<String> listOfApplicableTechPacks = new ArrayList<String>();
        if (typesRestrictedToOneTechPack.containsKey(type)) {
            listOfApplicableTechPacks.add(typesRestrictedToOneTechPack.get(type));
        }
        return listOfApplicableTechPacks;

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

}
