/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicy;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.RequestParametersWrapper;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Implementation of the business logic required for the MultipleRankingResource class/queries
 *
 * @author EEMECOY
 */
@Stateless
@Local(Service.class)
public class MultipleRankingService extends GenericService {

    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final String type = new RequestParametersWrapper(requestParameters).getType();
        templateParameters.put(COLUMNS_FOR_QUERY, TechPackData.aggregationColumns.get(type));
        return templateParameters;
    }

    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> extraQueryParameters = new HashMap<String, QueryParameter>();
        final String type = new RequestParametersWrapper(requestParameters).getType();
        if (type.equals(TYPE_APN)) {
            final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
            final ApplicationConfigManager appMgr = getApplicationConfigManager();
            final String apnRetentionInDays = Integer.toString(appMgr.getAPNRetention());
            final FormattedDateTimeRange dateTimeRangeForApn = DateTimeRange.getFormattedTimeRangeInDays(tzOffset,
                    apnRetentionInDays);

            final QueryParameter apnRequestParameter = QueryParameter.createStringParameter(dateTimeRangeForApn
                    .getStartDateTime());
            extraQueryParameters.put(DATE_FROM_FOR_APN_RETENTION, apnRequestParameter);
        }
        return extraQueryParameters;

    }

    @Override
    public String getTemplatePath() {
        return RANKING_ANALYSIS;
    }

    /**
     * The runQuery Method is overridden in this service, due to the parameter serviceSpecificDataServiceParameters used below to figure out what columns are time columns.
     * Because the time columns can not be hardcoded in this case, this override of runQuery is required
     * <p/>
     * Normally, individual services do not override runQuery.
     */
    @Override
    public String runQuery(final String query, final String requestId,
            final Map<String, QueryParameter> queryParameters, final LoadBalancingPolicy loadBalancingPolicy,
            final Map<String, Object> serviceSpecificDataServiceParameters) {
        final String timeColumn = (String) serviceSpecificDataServiceParameters.get(TIME_COLUMN);
        final String tzOffset = (String) serviceSpecificDataServiceParameters.get(TZ_OFFSET);
        return getDataService().getGridData(requestId, query, queryParameters, timeColumn, tzOffset,
                loadBalancingPolicy);
    }

    @Override
    public Map<String, Object> getServiceSpecificDataServiceParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, Object> dataServiceParameters = new HashMap<String, Object>();
        dataServiceParameters.put(TIME_COLUMN, null);
        dataServiceParameters.put(TZ_OFFSET, requestParameters.getFirst(TZ_OFFSET));
        return dataServiceParameters;
    }

    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(TYPE_PARAM);
        requiredParameters.add(DISPLAY_PARAM);
        return requiredParameters;
    }

    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        return staticParameters;
    }

    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final String type = new RequestParametersWrapper(requestParameters).getType();
        return Arrays.asList(getTechPacksPerRequestTypeMap().get(type));

    }

    private Map<String, String[]> getTechPacksPerRequestTypeMap() {
        final Map<String, String[]> techPacksPerRequestType = new HashMap<String, String[]>();
        techPacksPerRequestType.put(TYPE_APN, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_CAUSE_CODE, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_CELL, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_TAC, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_BSC, new String[] { EVENT_E_SGEH });
        techPacksPerRequestType.put(TYPE_RNC, new String[] { EVENT_E_SGEH });
        techPacksPerRequestType.put(TYPE_ENODEB, new String[] { EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_ECELL, new String[] { EVENT_E_LTE });
        return techPacksPerRequestType;
    }

    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return getAggregationViews().get(type);
    }

    private Map<String, AggregationTableInfo> getAggregationViews() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(APN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_TAC, new AggregationTableInfo(MANUF_TAC, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CAUSE_CODE, new AggregationTableInfo(EVNTSRC_CC, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(VEND_HIER3, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_RNC, new AggregationTableInfo(VEND_HIER3, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_ENODEB, new AggregationTableInfo(VEND_HIER3, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new AggregationTableInfo(VEND_HIER321, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_ECELL, new AggregationTableInfo(VEND_HIER321, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        return aggregationViews;
    }

    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS;
    }

    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        //raw tables are required for all ranking queries
        return true;
    }

    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        return false;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getTableSuffixKey()
     */
    @Override
    public String getTableSuffixKey() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getMeasurementTypes()
     */
    @Override
    public List<String> getMeasurementTypes() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getRawTableKeys()
     */
    @Override
    public List<String> getRawTableKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isDataTieredService(final MultivaluedMap<String, String> parameters) {
        if (parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_BSC)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_RNC)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_ENODEB)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_CAUSE_CODE)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_TAC)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_APN)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_CELL)
                || parameters.getFirst(TYPE_PARAM).equalsIgnoreCase(TYPE_ECELL)) {
            return applicationConfigManager.isDataTieringEnabled();
        }

        return false;
    }
}
