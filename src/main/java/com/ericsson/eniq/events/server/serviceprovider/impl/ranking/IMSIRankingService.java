/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.serviceprovider.impl.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;

import java.util.*;

import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.datasource.loadbalancing.LoadBalancingPolicy;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.RequestParametersWrapper;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@Stateless
@Local(Service.class)
public class IMSIRankingService extends GenericService {
	private static final String IMSI_TABLE_MEASURETYPE_KEY = "IMSI";

	@Override
	public Map<String, Object> getServiceSpecificTemplateParameters(
			final MultivaluedMap<String, String> requestParameters,
			final FormattedDateTimeRange dateTimeRange,
			final TechPackList techPackList) {
		final Map<String, Object> templateParameters = new HashMap<String, Object>();
		final String type = new RequestParametersWrapper(requestParameters)
				.getType();
		templateParameters.put(COLUMNS_FOR_QUERY,
				TechPackData.aggregationColumns.get(type));
		return templateParameters;

	}

	@Override
	public Map<String, QueryParameter> getServiceSpecificQueryParameters(
			final MultivaluedMap<String, String> requestParameters) {
		final Map<String, QueryParameter> extraQueryParameters = new HashMap<String, QueryParameter>();
		return extraQueryParameters;

	}

	@Override
	public String getTemplatePath() {
		return RANKING_ANALYSIS_IMSI;
	}

	/**
	 * The runQuery Method is overridden in this service, due to the parameter
	 * serviceSpecificDataServiceParameters used below to figure out what
	 * columns are time columns. Because the time columns can not be hardcoded
	 * in this case, this override of runQuery is required
	 * <p/>
	 * Normally, individual services do not override runQuery.
	 */
	@Override
	public String runQuery(final String query, final String requestId,
			final Map<String, QueryParameter> queryParameters,
			final LoadBalancingPolicy loadBalancingPolicy,
			final Map<String, Object> serviceSpecificDataServiceParameters) {
		final String timeColumn = (String) serviceSpecificDataServiceParameters
				.get(TIME_COLUMN);
		final String tzOffset = (String) serviceSpecificDataServiceParameters
				.get(TZ_OFFSET);
		return getDataService().getGridData(requestId, query, queryParameters,
				timeColumn, tzOffset, loadBalancingPolicy);
	}

	@Override
	public Map<String, Object> getServiceSpecificDataServiceParameters(
			final MultivaluedMap<String, String> requestParameters) {
		final Map<String, Object> dataServiceParameters = new HashMap<String, Object>();
		dataServiceParameters.put(TIME_COLUMN, null);
		dataServiceParameters.put(TZ_OFFSET,
				requestParameters.getFirst(TZ_OFFSET));
		return dataServiceParameters;
	}

	@Override
	public List<String> getRequiredParametersForQuery() {
		final List<String> requiredParameters = new ArrayList<String>();
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
	public List<String> getApplicableTechPacks(
			final MultivaluedMap<String, String> requestParameters) {
        		return Arrays.asList(new String[] { EVENT_E_LTE, EVENT_E_SGEH });

	}

	@Override
	public AggregationTableInfo getAggregationView(final String type) {
		return new AggregationTableInfo(NO_TABLE);
	}

	@Override
	public String getDrillDownTypeForService(
			final MultivaluedMap<String, String> requestParameters) {
		return null;
	}

	@Override
	public int getMaxAllowableSize() {
		return MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS;
	}

	@Override
	public boolean areRawTablesRequiredForAggregationQueries() {
		return true;
	}

	@Override
	public boolean requiredToCheckValidParameterValue(
			final MultivaluedMap<String, String> requestParameters) {
		return false;
	}

	@Override
	public String getTableSuffixKey() {
		return SUC;
	}

	@Override
	public List<String> getMeasurementTypes() {
        List<String> measurementTypes = new ArrayList<String>();
        measurementTypes.add(IMSI_TABLE_MEASURETYPE_KEY);
        return measurementTypes;
	}

	@Override
	public List<String> getRawTableKeys() {
		return null;
	}

	@Override
	public boolean isDataTieredService(
			final MultivaluedMap<String, String> parameters) {

		return applicationConfigManager.isDataTieringEnabled();

	}
}
