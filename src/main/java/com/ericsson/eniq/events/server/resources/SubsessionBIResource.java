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
import static com.ericsson.eniq.events.server.utils.SubsessionBIUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
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

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.json.JSONException;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/** 
 * The Class SubsessionBIResource.
 *
 * @author eavidat
 * @author ehaoswa
 * @since  Aug 2010
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class SubsessionBIResource extends BaseResource {

    /**
     * Constructor for class
     * Initializing the look up maps - cannot have these as static, or initialized in a static block, as they
     * can be initialized from several classes
     */
    public SubsessionBIResource() {
        typesRestrictedToOneTechPack = new HashMap<String, String>();
        typesRestrictedToOneTechPack.put(TYPE_PTMSI, TechPackData.EVENT_E_SGEH);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the APN data for SUB BI.
     *
     * @return the APN data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_APN)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIAPNData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_APN, null, APN_PARAM, SUBBI_APN);
    }

    /**
     * Gets the Busy Day data for SUB BI.
     *
     * @return the Busy Day data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_BUSY_DAY)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIBusyDayData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_BUSY_DAY, SUBBI_BUSY_DAY, DAY_PARAM,
                SUBBI_BUSY_DAY);
    }

    /**
     * Gets the Busy Hour data for SUB BI.
     *
     * @return the Busy Hour data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_BUSY_HOUR)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIBusyHourData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_BUSY_HOUR, SUBBI_BUSY_HOUR, HOUR_PARAM,
                SUBBI_BUSY_HOUR);
    }

    /**
     * Gets the Cell data for SUB BI.
     *
     * @return the Cell data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_CELL)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBICellData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_CELL, null, NODE_PARAM, SUBBI_CELL);
    }

    /**
     * Gets the Failure data for SUB BI.
     *
     * @return the Failure data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_FAILURE)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIFailureData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_FAILURE, null, NODE_PARAM, SUBBI_FAILURE);
    }

    /**
     * Gets the Terminal data for SUB BI.
     *
     * @return the Terminal data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_TERMINAL)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBITerminalData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_TERMINAL, null, TAC_PARAM, SUBBI_TERMINAL);
    }

    /**
     * Gets the TAU Events data for SUB BI
     *
     * @return the TAU data for SUB BI
     * @throws WebApplicationException the web application exception
     */
    @Path(SUBBI_TAU)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBITAUData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_TAU, null, NODE_PARAM, SUBBI_TAU);
    }

    /**
     * Gets the HANDOVER Events data for SUB BI
     *
     * @return the HANDOVER data for SUB BI
     * @throws WebApplicationException the web application exception
     */
    @Path(SUBBI_HANDOVER)
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaTypeConstants.APPLICATION_CSV })
    public String getSubBIHandoverData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_HANDOVER, null, NODE_PARAM, SUBBI_HANDOVER);
    }

    /**
     * Gets the Subscriber details data for SUB BI.
     *
     * @return the Subscriber details data for SUB BI
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    @Path(SUBBI_SUBSCRIBER_DETAILS)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubBISubscriberDetailsData() throws WebApplicationException, JSONException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRequestData(requestId, getDecodedQueryParameters(), SUBBI_SUBSCRIBER_DETAILS, null, null, null);
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

    /**
     * Gets the request data.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param templateQuery the template query
     * @param busyKey the key to differentiate between busy hour and busy day
     * @param drilldownKey the drilldown key
     * @param drilldownType the velocity template type for a drilldown
     * @return the request data
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    private String getRequestData(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final String subPath, final String busyKey, final String drilldownKey, final String drilldownType)
            throws WebApplicationException, JSONException {

        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final StringBuffer sb = new StringBuffer();
        final String pathName = sb.append(SUBBI).append("/").append(subPath).toString();
        String drillType = null;
        if (drilldownKey != null && requestParameters.containsKey(drilldownKey)) {
            drillType = drilldownType;
        }

        return getSubBIDataResults(requestId, requestParameters, getTemplate(pathName, requestParameters, drillType),
                busyKey, drillType, subPath);
    }

    /**
     * Gets the display specific JSON results for SubBI.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param query the SQL query
     * @param newDateTimeRange the date time range of chart and grid
     * @param timeColumn the timezone column
     * @param busyKey the key to differentiate between busy hour and busy day
     * @param drilldownType the velocity template type for a drilldown
     *
     * @return JSON encoded results
     */

    private String getDisplaySpecificResults(final String requestId,
            final MultivaluedMap<String, String> requestParameters, final String query,
            final FormattedDateTimeRange newDateTimeRange, final String timeColumn, final String busyKey,
            final String drilldownType) {

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        String results = null;

        if (isChart(displayType)) {
            if (isBusyType(busyKey)) {
                results = this.dataService.getSubBIBusyData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange),
                        getLoadBalancingPolicy(requestParameters), busyKey, requestParameters.getFirst(TZ_OFFSET));
            } else {
                results = this.dataService.getChartData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), SUBBI_X_AXIS_VALUE,
                        null, timeColumn, requestParameters.getFirst(TZ_OFFSET),
                        getLoadBalancingPolicy(requestParameters));
            }

        } else if (isGrid(displayType)) {
            if (drilldownType == null && isBusyDay(busyKey)) {
                results = this.dataService.getSubBIBusyDayGridData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), timeColumn,
                        getLoadBalancingPolicy(requestParameters), requestParameters.getFirst(TZ_OFFSET));
            } else {
                results = this.dataService.getGridData(requestId, query,
                        this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), timeColumn,
                        requestParameters.getFirst(TZ_OFFSET), getLoadBalancingPolicy(requestParameters));
            }
        }
        return results;
    }

    /**
     * @param busyKey
     * @param drilldownType
     * @return
     */
    private boolean isBusyDay(final String busyKey) {
        return isBusyType(busyKey) && busyKey.equalsIgnoreCase(SUBBI_BUSY_DAY);
    }

    /**
     * Gets the JSON results for SubBI - updates the parameters, gets the query and calls getDisplaySpecificResults.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param templateQuery the template query
     * @param busyKey the key to differentiate between busy hour and busy day
     * @param drilldownType the velocity template type for a drilldown
     * @return JSON encoded results
     * @throws WebApplicationException the web application exception
     * @throws JSONException the JSON exception
     */
    private String getSubBIDataResults(final String requestId, final MultivaluedMap<String, String> requestParameters,
            final String templateQuery, final String busyKey, final String drilldownType, final String subPath)
            throws WebApplicationException, JSONException {

        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        //need to do this before the meddling with the request parameters directly below
        templateParameters.put(TYPE_PARAM, requestParameters.getFirst(TYPE_PARAM));
        final String timeColumn;
        if (null != drilldownType) {
            templateParameters.put(DRILLTYPE_PARAM, drilldownType);
            requestParameters.putSingle(TYPE_PARAM, drilldownType);
            timeColumn = EVENT_TIME_COLUMN_INDEX;
        } else {
            templateParameters.put(TYPE_PARAM, requestParameters.getFirst(TYPE_PARAM));
            timeColumn = null;
        }

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS));

        final List<String> techPackList = new ArrayList<String>();
        techPackList.add(EVENT_E_SGEH);
        techPackList.add(EVENT_E_LTE);
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, techPackList);

        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);
        updateTemplateForBusyKey(busyKey, templateParameters, requestParameters, drilldownType);
        updateTemplateWithAppropriateEventType(subPath, templateParameters);

        if (!updateTemplateWithAppropriateRAWTables(subPath, templateParameters, requestParameters, dateTimeRange,
                drilldownType)) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        final String query = templateUtils.getQueryFromTemplate(templateQuery, templateParameters);
        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        final FormattedDateTimeRange newDateTimeRange = instantiateNewDateTimeRange(subPath, dateTimeRange);
        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, newDateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), timeColumn, query,
                    newDateTimeRange);
            return null;
        }

        final String result = getDisplaySpecificResults(requestId, requestParameters, query, newDateTimeRange,
                timeColumn, busyKey, drilldownType);

        return updateJSONWithTimeZoneOffset(subPath, result, requestParameters, drilldownType);

    }

    private FormattedDateTimeRange instantiateNewDateTimeRange(final String subPath,
            final FormattedDateTimeRange dateTimeRange) {
        final FormattedDateTimeRange newDateTimeRange;
        if (subPath.equals(SUBBI_SUBSCRIBER_DETAILS)) {
            newDateTimeRange = null;
        } else {
            newDateTimeRange = dateTimeRange;
        }
        return newDateTimeRange;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(GROUP_NAME_PARAM) || isImsiOrImsiGroupQuery(requestParameters)
                || requestParameters.containsKey(IMSI_PARAM) || requestParameters.containsKey(PTMSI_PARAM)
                || requestParameters.containsKey(MSISDN_PARAM)) {

            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method is used to update the templateParameters map with appropriate RAW tables for a specific time range
     * 
     * @param subPath the URI sub path
     * @param templateParameters the template parameters
     * @param requestParameters the request parameters
     * @param dateTimeRange the date time range
     * 
     * @return false if there is no RAW table for a specific time range
     */
    private boolean updateTemplateWithAppropriateRAWTables(final String subPath,
            final Map<String, Object> templateParameters, final MultivaluedMap<String, String> requestParameters,
            final FormattedDateTimeRange dateTimeRange, final String drilldownType) {
        final String type = requestParameters.getFirst(TYPE_PARAM);
        final TechPackTables techPackTables = getRawTables(type, dateTimeRange, TechPackData.completeSGEHTechPackList);
        templateParameters.put(TECH_PACK_TABLES, techPackTables);

        if (drilldownType != null) {
            return updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_ERR, RAW_NON_LTE_TABLES,
                    RAW_LTE_TABLES);
        } else if (subPath.equals(SUBBI_SUBSCRIBER_DETAILS) || subPath.equals(SUBBI_TAU)
                || subPath.equals(SUBBI_HANDOVER)) {
            final TechPackTables techPackTablesSGEH = getRawTables(type, dateTimeRange,
                    Arrays.asList(new String[] { TechPackData.EVENT_E_SGEH }));
            final TechPackTables techPackTablesLTE = getRawTables(type, dateTimeRange,
                    Arrays.asList(new String[] { TechPackData.EVENT_E_LTE }));
            templateParameters.put(TECH_PACK_TABLES_SGEH, techPackTablesSGEH);
            templateParameters.put(TECH_PACK_TABLES_LTE, techPackTablesLTE);
            if (subPath.equals(SUBBI_SUBSCRIBER_DETAILS)) {
                return isTechPackTablesValid(techPackTablesSGEH) ? true : isTechPackTablesValid(techPackTablesLTE);
            }
            return isTechPackTablesValid(techPackTablesSGEH) && type == TYPE_PTMSI ? true
                    : isTechPackTablesValid(techPackTablesLTE);
        } else {
            return isTechPackTablesValid(techPackTables);
        }
    }

    /**
     * This method checks if the TechPackTables object passed in is valid.
     * The object is valid if it is not null, and the size of the success and error tables 
     * lists it returns are not zero.
     * 
     * @param techPackTables
     * @return
     */
    private boolean isTechPackTablesValid(final TechPackTables techPackTables) {
        return techPackTables != null && techPackTables.getSucTables() != null && techPackTables.getErrTables() != null
                && techPackTables.getSucTables().size() != 0 && techPackTables.getErrTables().size() != 0;
    }

    /**
     * This method is used to update templateParameters map with appropriate event type e.g. L_TAU or L_HANDOVER
     * 
     * @param subPath the URI path
     * @param templateParameters the template parameters
     * 
     */
    private void updateTemplateWithAppropriateEventType(final String subPath,
            final Map<String, Object> templateParameters) {
        if (subPath.equals(SUBBI_TAU)) {
            templateParameters.put(EVENT_TYPE_PARAM, L_TAU);
        } else if (subPath.equals(SUBBI_HANDOVER)) {
            templateParameters.put(EVENT_TYPE_PARAM, L_HANDOVER);
        }
    }

    /**
     * This method is used to update templateParameters map with busy key specific values
     
     * @param busyKey the busy key parameter
     * @param templateParameters the template parameters
     * @param requestParameters the request parameters
     * @param drilldownType the drill down type
     * 
     */
    private void updateTemplateForBusyKey(final String busyKey, final Map<String, Object> templateParameters,
            final MultivaluedMap<String, String> requestParameters, final String drilldownType) {
        if (null != busyKey) {
            templateParameters.put(SUBBI_BUSY_KEY, busyKey);
        }
        if (SUBBI_BUSY_DAY.equals(busyKey)) {
            final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
            templateParameters.put(TZ_OFFSET, getTZOffsetMinutes(tzOffset));
        } else if (SUBBI_BUSY_HOUR.equals(busyKey) && null != drilldownType) {
            final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
            final String hourString = requestParameters.getFirst(HOUR_PARAM);
            final String hourWithTz = getHourParameterWithTZOffset(hourString, tzOffset);
            requestParameters.putSingle(HOUR_PARAM, hourWithTz);
        }
    }

    /**
     * This method is used to update the result with time zone offset value
     * 
     * @param subPath the URI path
     * @param result the JSON returned from database query
     * @param requestParameters the request parameters
     * @param drilldownType the drill down key
     * 
     * @return the update JSON
     * 
     * @throws JSONException
     */
    private String updateJSONWithTimeZoneOffset(final String subPath, final String result,
            final MultivaluedMap<String, String> requestParameters, final String drilldownType) throws JSONException {
        if (subPath.equals(SUBBI_SUBSCRIBER_DETAILS)) {
            if (isImsiOrImsiGroupQuery(requestParameters)) {
                return updateTimeWithTimeZoneOffset(result, requestParameters.getFirst(TZ_OFFSET),
                        new String[] { IMSI_SUBBI_DETAILS_FIRST_SEEN_TIME_COLUMN_INDEX,
                                IMSI_SUBBI_DETAILS_LAST_SEEN_TIME_COLUMN_INDEX });
            }
            return updateTimeWithTimeZoneOffset(result, requestParameters.getFirst(TZ_OFFSET), new String[] {
                    PTMSI_SUBBI_DETAILS_FIRST_SEEN_TIME_COLUMN_INDEX, PTMSI_SUBBI_DETAILS_LAST_SEEN_TIME_COLUMN_INDEX });
        }
        if (subPath.equals(SUBBI_TERMINAL) && StringUtils.isEmpty(drilldownType)) {
            return updateTimeWithTimeZoneOffset(result, requestParameters.getFirst(TZ_OFFSET), new String[] {
                    SUBBI_TERMINAL_FIRST_SEEN_TIME_COLUMN_INDEX, SUBBI_TERMINAL_LAST_SEEN_TIME_COLUMN_INDEX });
        }

        return result;
    }
}
