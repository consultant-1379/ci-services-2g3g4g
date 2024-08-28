/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.roaminganalysis;

import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_LTE;
import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_SGEH;

/**
 * Implementation of roaming analysis drill service.Drill by operator name.Group
 * by EventID.
 * 
 * @author ezhelao
 * @since 01/2012
 */
@Stateless
@Local(Service.class)
public class RoamingDrillByOperatorService extends GenericService {

	@Override
	public String getTemplatePath() {
		return ROAMING_ANALYSIS_DRILL_OPERATOR;
	}

	@Override
	public Map<String, Object> getServiceSpecificTemplateParameters(
			final MultivaluedMap<String, String> requestParameters,
			final FormattedDateTimeRange dateTimeRange,
			final TechPackList techPackList) {
		final HashMap<String, Object> params = new HashMap<String, Object>();

		params.put(MCC_PARAM, requestParameters.getFirst(MCC_PARAM));
		params.put(MNC_PARAM, requestParameters.getFirst(MNC_PARAM));
		return params;

	}

	@Override
	public Map<String, Object> getServiceSpecificDataServiceParameters(
			final MultivaluedMap<String, String> requestParameters) {
		final Map<String, Object> params = new HashMap<String, Object>();
		params.put(TZ_OFFSET, requestParameters.getFirst(TZ_OFFSET));
		return params;
	}

	@Override
	public Map<String, QueryParameter> getServiceSpecificQueryParameters(
			final MultivaluedMap<String, String> requestParameters) {
		final Map<String, QueryParameter> dataServiceParameters = new HashMap<String, QueryParameter>();
		dataServiceParameters.put(MCC_PARAM, QueryParameter
				.createStringParameter(String.valueOf(requestParameters
						.getFirst(MCC_PARAM))));
		dataServiceParameters.put(MNC_PARAM, QueryParameter
				.createStringParameter(String.valueOf(requestParameters
						.getFirst(MNC_PARAM))));
		dataServiceParameters.put(OPERATOR, QueryParameter
				.createStringParameter(String.valueOf(requestParameters
						.getFirst(OPERATOR))));
		return dataServiceParameters;
	}

	@Override
	public List<String> getRequiredParametersForQuery() {
		final List<String> params = new ArrayList<String>();
		params.add(MCC_PARAM);
		params.add(MNC_PARAM);
		return params;
	}

	@Override
	public MultivaluedMap<String, String> getStaticParameters() {
		return new MultivaluedMapImpl();
	}

	@Override
	public String getDrillDownTypeForService(
			final MultivaluedMap<String, String> requestParameters) {
		return null; // To change body of implemented methods use File |
						// Settings | File Templates.
	}

	@Override
	public AggregationTableInfo getAggregationView(final String type) {
		return new AggregationTableInfo(NO_AGGREGATION_AVAILABLE);
	}

	@Override
	public List<String> getApplicableTechPacks(
			final MultivaluedMap<String, String> requestParameters) {
		final List<String> techPacks = new ArrayList<String>();
		techPacks.add(EVENT_E_SGEH);
		techPacks.add(EVENT_E_LTE);
		return techPacks;
	}

	@Override
	public boolean areRawTablesRequiredForAggregationQueries() {
		return true;
	}

	@Override
	public int getMaxAllowableSize() {
		return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
	}

	@Override
	public boolean requiredToCheckValidParameterValue(
			final MultivaluedMap<String, String> requestParameters) {
		return false; // To change body of implemented methods use File |
						// Settings | File Templates.
	}

	@Override
	public String getTableSuffixKey() {
		return null;
	}

	@Override
	public List<String> getMeasurementTypes() {
		return null;
	}

	@Override
	public List<String> getRawTableKeys() {
		final List<String> keys = new ArrayList<String>();
		keys.add(ERR);
		return keys;
	}
}
