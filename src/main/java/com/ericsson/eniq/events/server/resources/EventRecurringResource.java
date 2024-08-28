/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * Sub-resource for event recurring detail and summary request handling.
 *
 * @author eavidat 
 * 
 * @since  Jan 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
@SuppressWarnings("PMD.CyclomaticComplexity")
public class EventRecurringResource extends BaseResource {

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
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
        if (!requestParameters.containsKey(EVENT_TYPE_PARAM)) {
            errors.add(EVENT_TYPE_PARAM);
        }
        if (!requestParameters.containsKey(CAUSE_CODE_PARAM)) {
            errors.add(CAUSE_CODE_PARAM);
        }
        if (!requestParameters.containsKey(SUB_CAUSE_CODE_PARAM)) {
            errors.add(SUB_CAUSE_CODE_PARAM);
        }
        if (!requestParameters.containsKey(EVENT_RESULT_PARAM)) {
            errors.add(EVENT_RESULT_PARAM);
        }
        if (!requestParameters.containsKey(CAUSE_PROT_TYPE)) {
            errors.add(CAUSE_PROT_TYPE);
        }

        return errors;
    }

    /**
     * Gets the total or detail data for recurring events
     *
     * @return the detail data of recurring events
     * @throws WebApplicationException the web application exception
     */
    @Path(KEY_TYPE_TOTAL)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRecurErrorTotalData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), KEY_TYPE_TOTAL);
    }

    /**
     * Gets the summary data for recurring events
     *
     * @return the summary of recurring events data
     * @throws WebApplicationException the web application exception
     */
    @Path(KEY_TYPE_SUM)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRecurErrorSummaryData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), KEY_TYPE_SUM);
    }

    /**
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters : the URL parameters
     * @param key               : the template parameter
     * 
     * @return recurring error data 
     * 
     * @throws WebApplicationException
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final String key) throws WebApplicationException {
        final String drillType = null;
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        final List<String> techPackList = new ArrayList<String>();
        techPackList.add(EVENT_E_SGEH);
        techPackList.add(EVENT_E_LTE);
        final FormattedDateTimeRange newDateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, techPackList);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final String timeColumn;
        if (KEY_TYPE_TOTAL.equals(key)) {
            timeColumn = EVENT_TIME_COLUMN_INDEX;
        } else {
            timeColumn = null;
            templateParameters.put(PARAM_HEADERS, getParamHeadersForTemplate(requestParameters));
        }

        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        if (!updateTemplateWithRAWTables(templateParameters, newDateTimeRange, KEY_TYPE_ERR, RAW_NON_LTE_TABLES,
                RAW_LTE_TABLES)) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        templateParameters.put(TYPE_PARAM, requestParameters.getFirst(TYPE_PARAM));
        templateParameters.put(COUNT_PARAM,
                getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));

        updateRequestParametersForNonIMSIType(requestParameters);

        templateParameters.put(KEY_PARAM, key);

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(EVENT_RECURRING, requestParameters, drillType), templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, newDateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumn, query, newDateTimeRange);
            return null;
        }

        return this.dataService.getGridData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), timeColumn, tzOffset,
                getLoadBalancingPolicy(requestParameters));

    }

    /**
     * @param requestParameters : the URL parameters
     * @param key               : the template parameter
     * 
     * @return recurring error data using getGridResults method
     * 
     * @throws WebApplicationException
     */
    private String getRequestData(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final String key) throws WebApplicationException {

        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        return getGridResults(requestId, requestParameters, key);
    }

    /**
     * @param requestParameters : the URL parameters
     * 
     * @return concatenated string containing parameters passed down from UI during summary view
     * 
     * this concatenated string is used in template in SQL query result
     * 
     */
    private String getParamHeadersForTemplate(final MultivaluedMap<String, String> requestParameters) {

        final StringBuilder paramHeaders = new StringBuilder();
        paramHeaders.append(QUOTE_SINGLE).append(requestParameters.getFirst(EVENT_TYPE_PARAM)).append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(CAUSE_PROT_TYPE_HEADER))
                .append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(CAUSE_CODE_HEADER))
                .append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(SUB_CAUSE_CODE_HEADER))
                .append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(CAUSE_CODE_PARAM))
                .append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(SUB_CAUSE_CODE_PARAM))
                .append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(EVENT_RESULT_PARAM))
                .append(QUOTE_SINGLE);
        paramHeaders.append(COMMA).append(QUOTE_SINGLE).append(requestParameters.getFirst(CAUSE_PROT_TYPE))
                .append(QUOTE_SINGLE);

        requestParameters.remove(CAUSE_PROT_TYPE_HEADER);
        requestParameters.remove(CAUSE_CODE_HEADER);
        requestParameters.remove(SUB_CAUSE_CODE_HEADER);

        return paramHeaders.toString();
    }

    /**
     * @param requestParameters  : the URL parameters - which will be updated accordingly
     */
    private void updateRequestParametersForNonIMSIType(final MultivaluedMap<String, String> requestParameters) {
        if (TYPE_IMSI.equals(requestParameters.getFirst(TYPE_PARAM))) {
            return;
        }
        if (requestParameters.containsKey(PTMSI_PARAM)) {
            requestParameters.remove(PTMSI_PARAM);
        }
        if (requestParameters.containsKey(TAC_PARAM)) {
            requestParameters.remove(TAC_PARAM);
        }
        if (requestParameters.containsKey(NODE_PARAM)) {
            requestParameters.remove(NODE_PARAM);
        }
        if (requestParameters.containsKey(BSC_PARAM)) {
            requestParameters.remove(BSC_PARAM);
        }
        if (requestParameters.containsKey(VENDOR_PARAM)) {
            requestParameters.remove(VENDOR_PARAM);
        }
        if (requestParameters.containsKey(RAT_PARAM)) {
            requestParameters.remove(RAT_PARAM);
        }
        if (requestParameters.containsKey(KEY_PARAM)) {
            requestParameters.remove(KEY_PARAM);
        }
        if (requestParameters.containsKey(CELL_PARAM)) {
            requestParameters.remove(CELL_PARAM);
        }
        if (requestParameters.containsKey(APN_PARAM)) {
            requestParameters.remove(APN_PARAM);
        }
        if (requestParameters.containsKey(SGSN_PARAM)) {
            requestParameters.remove(SGSN_PARAM);
        }
        if (!requestParameters.containsKey(GROUP_NAME_PARAM) && requestParameters.containsKey(IMSI_HEADER)) {
            requestParameters.putSingle(IMSI_PARAM, requestParameters.getFirst(IMSI_HEADER));
            requestParameters.remove(IMSI_HEADER);
        }
        if (requestParameters.containsKey(MAN_PARAM)) {
            requestParameters.remove(MAN_PARAM);
        }
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(IMSI_PARAM) || requestParameters.containsKey(PTMSI_PARAM)
                || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }
}