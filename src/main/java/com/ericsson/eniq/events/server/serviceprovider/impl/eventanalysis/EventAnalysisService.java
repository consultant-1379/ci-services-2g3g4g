/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis;

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

import org.apache.commons.lang.StringUtils;

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
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Implementation of the business logic required for the EventAnalysisResource class/queries
 *
 * @author EEIKONL
 */
@Stateless
@Local(Service.class)
public class EventAnalysisService extends GenericService {
    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(requestParameters);
        if(TYPE_APN.equals(requestParametersWrapper.getType())){
        	 templateParameters.put("APN_Value",requestParameters.getFirst(NODE_PARAM));
        }
        templateParameters.put(KEY_PARAM, requestParametersWrapper.getKey());
        templateParameters.put(COLUMNS_FOR_QUERY,
                TechPackData.aggregationColumns.get(requestParametersWrapper.getType()));
        templateParameters.put(EVENT_ID_PARAM, requestParametersWrapper.getEventId());
        templateParameters.put(GROUP_NAME_PARAM, requestParametersWrapper.getGroupName());
        templateParameters.put(USE_TAC_EXCLUSION_BOOLEAN, getExclusiveTACHandler()
                .shouldUseTACExclusionInSGEHEventAnalysisQuery(requestParameters));

        if (requestParametersWrapper.getKey().equals(KEY_TYPE_SUM)) {
            templateParameters.put(TIMEWINDOW_PARAM, DateTimeRange.getTimewindowValue(dateTimeRange));
        }
        if (requestParameters.containsKey(QCI_ID)) {
            final String qciId = requestParameters.getFirst(QCI_ID);
            templateParameters.put(QCI_ERR_FILTER, transformQCIParameter(qciId));
        }
        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);
        return templateParameters;

    }

    /**
     * This method will not add any parameters, but instead will remove the Event_ID from the Request Parameters
     * if the key is err, suc or total. This is done because the template has already been updated with the
     * event_ids
     *
     * @param requestParameters request Parameters provided by user
     * @return the additional query Parameters that should be added on query execution
     */
    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(requestParameters);
        final String key = requestParametersWrapper.getKey();
        if (key.equals(KEY_TYPE_ERR) || key.equals(KEY_TYPE_SUC) || key.equals(KEY_TYPE_TOTAL)) {
            //have input event id into template already, no need to pass into query again
            requestParameters.remove(EVENT_ID_PARAM);
        }
        updateRequestParameters(requestParameters);
        return new HashMap<String, QueryParameter>();
    }

    /**
     * Data tiering selecting err raw table and suc 15min table for upto TR_1 queries is not 
     * applicable for IMSI relevant queries as we don't have any 15min table associated with subscriber
     * Also if "dataTieredDelay=true" is set from URL it means the a user drill down on failed link 
     * from where we already applied data tiering latency. 
     */
    @Override
    public boolean isDataTieredService(final MultivaluedMap<String, String> parameters) {
        if (parameters.getFirst(KEY_DATA_TIERED_DELAY) != null
                || !parameters.getFirst(KEY_PARAM).equalsIgnoreCase(KEY_TYPE_SUM)
                || parameters.getFirst(TYPE).equalsIgnoreCase(IMSI)) {
            return false;
        }

        return applicationConfigManager.isDataTieringEnabled();
    }

    /**
     * This method will update the request parameters by modifying the NODE_PARAM based on type
     *
     * @param requestParameters
     */
    private void updateRequestParameters(final MultivaluedMap<String, String> requestParameters) {
        final String node = requestParameters.getFirst(NODE_PARAM);
        if (StringUtils.isBlank(node)) {
            return;
        }
        final String type = requestParameters.getFirst(TYPE_PARAM);
        if (type.equals(TYPE_APN)) {
            requestParameters.putSingle(APN_PARAM, node);
        } else if (type.equals(TYPE_SGSN)) {
            requestParameters.putSingle(SGSN_PARAM, node);
        } else if (type.equals(TYPE_TAC)) {
            final String[] value = node.split(DELIMITER);
            requestParameters.putSingle(TAC_PARAM, value[value.length - 1]);
        } else if (type.equals(TYPE_BSC)) {
            final String[] value = node.split(DELIMITER);
            requestParameters.putSingle(BSC_PARAM, value[0]);
            requestParameters.putSingle(VENDOR_PARAM, value[1]);
            requestParameters.putSingle(RAT_PARAM, getQueryUtils().getRATValueAsInteger(requestParameters));
        } else if (type.equals(TYPE_CELL)) {
            final String[] value = node.split(DELIMITER);
            requestParameters.putSingle(CELL_PARAM, value[0]);
            requestParameters.putSingle(BSC_PARAM, value[2]);
            requestParameters.putSingle(VENDOR_PARAM, value[3]);
            requestParameters.putSingle(RAT_PARAM, getQueryUtils().getRATValueAsInteger(requestParameters));
        }
    }

    @Override
    public String getTemplatePath() {
        return EVENT_ANALYSIS;
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
        String timeColumn = null;
        final Map<String, Object> dataServiceParameters = new HashMap<String, Object>();
        final String key = requestParameters.getFirst(KEY_PARAM);

        if (key.equals(KEY_TYPE_ERR) || key.equals(KEY_TYPE_SUC) || key.equals(KEY_TYPE_TOTAL)) {
            timeColumn = EVENT_TIME_COLUMN_INDEX;
        }

        dataServiceParameters.put(TIME_COLUMN, timeColumn);
        dataServiceParameters.put(TZ_OFFSET, requestParameters.getFirst(TZ_OFFSET));
        return dataServiceParameters;
    }

    /**
     * Given the string QCI_2, returns QCI_ERR_2
     */
    String transformQCIParameter(final String qciId) {
        return QCI_ + ERR + UNDERSCORE + qciId;
    }

    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(KEY_PARAM);
        requiredParameters.add(TYPE_PARAM);
        requiredParameters.add(DISPLAY_PARAM);
        return requiredParameters;
    }

    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.add(DISPLAY_PARAM, GRID_PARAM);
        return staticParameters;
    }

    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final String type = new RequestParametersWrapper(requestParameters).getType();
        final String key = new RequestParametersWrapper(requestParameters).getKey();
        String[] techPacksPerRequestType = getTechPacksPerRequestTypeMap(key).get(type);
        if (techPacksPerRequestType == null) {
            techPacksPerRequestType = new String[] {};
        }
        return Arrays.asList(techPacksPerRequestType);

    }

    private Map<String, String[]> getTechPacksPerRequestTypeMap(final String key) {
        final Map<String, String[]> techPacksPerRequestType = new HashMap<String, String[]>();

        if (KEY_TYPE_ERR.equals(key) || KEY_TYPE_SUC.equals(key) || KEY_TYPE_TOTAL.equals(key)) {
            techPacksPerRequestType.put(TYPE_APN, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_TAC, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_SGSN, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_IMSI, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_MSISDN, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DTPDP });
        } else if (KEY_TYPE_SUM.equals(key)) {
            techPacksPerRequestType.put(TYPE_APN, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DT,
                    EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_TAC, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DT,
                    EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_SGSN, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DT,
                    EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_IMSI, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DT,
                    EVENT_E_GSN_DTPDP });
            techPacksPerRequestType.put(TYPE_MSISDN, new String[] { EVENT_E_SGEH, EVENT_E_LTE, EVENT_E_GSN_DT,
                    EVENT_E_GSN_DTPDP });

        }
        techPacksPerRequestType.put(TYPE_BSC, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_CELL, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_CAUSE_CODE, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_MAN, new String[] { EVENT_E_SGEH, EVENT_E_LTE });
        techPacksPerRequestType.put(TYPE_PTMSI, new String[] { EVENT_E_SGEH, EVENT_E_LTE });

        return techPacksPerRequestType;
    }

    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return getAggregationViews().get(type);
    }

    private Map<String, AggregationTableInfo> getAggregationViews() {
        final Map<String, AggregationTableInfo> aggregationViews = new HashMap<String, AggregationTableInfo>();

        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(VEND_HIER3_EVENTID,
                EventDataSourceType.AGGREGATED_1MIN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(APN_EVENTID, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_TAC, new AggregationTableInfo(MANUF_TAC_EVENTID, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_SGSN, new AggregationTableInfo(EVNTSRC_EVENTID, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new AggregationTableInfo(VEND_HIER321_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));

        //Not sure if these are needed, the existing EventAnalysisResource, only seems to use the ones above
        aggregationViews.put(TYPE_CAUSE_CODE, new AggregationTableInfo(EVNTSRC_CC, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_RNC, new AggregationTableInfo(VEND_HIER3_EVENTID,
                EventDataSourceType.AGGREGATED_1MIN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_ENODEB, new AggregationTableInfo(VEND_HIER3_EVENTID,
                EventDataSourceType.AGGREGATED_1MIN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_ECELL, new AggregationTableInfo(VEND_HIER321_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_ECELL, new AggregationTableInfo(VEND_HIER321_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MAN, new AggregationTableInfo(MANUF_TAC_EVENTID,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));

        aggregationViews.put(TYPE_IMSI, new AggregationTableInfo(NO_TABLE));
        aggregationViews.put(TYPE_PTMSI, new AggregationTableInfo(NO_TABLE));
        aggregationViews.put(TYPE_MSISDN, new AggregationTableInfo(NO_TABLE));
        return aggregationViews;
    }

    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
    }

    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return true;
    }

    @Override
    /**
     * We need to make sure that the values passed are valid values, so indicate to calling class, that we
     * need value validation.
     */
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)
                || requestParameters.containsKey(IMSI_PARAM) || requestParameters.containsKey(PTMSI_PARAM)
                || requestParameters.containsKey(MSISDN_PARAM)) {
            return true;
        }
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
}