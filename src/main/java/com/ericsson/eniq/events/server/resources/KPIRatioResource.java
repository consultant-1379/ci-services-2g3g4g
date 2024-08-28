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
import java.util.Arrays;
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
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;
import com.ericsson.eniq.events.server.utils.techpacks.ViewTypeSelector;

/**
 * The Class KPIRatioResource This class handles new KPI drill down events.
 * 
 * @author ehaoswa
 * @since 2010 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class KPIRatioResource extends BaseResource {

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }
        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(GRID_PARAM)) {
            return getGridResults(requestId, requestParameters);
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
        if (!requestParameters.containsKey(EVENT_ID_PARAM)) {
            errors.add(EVENT_ID_PARAM);
        }
        return errors;
    }

    /**
     * Gets the grid results.
     *
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters the request parameters
     * @return the grid results
     * @throws WebApplicationException the web application exception
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {

        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        String timeColumn = null;
        final List<String> techPackList = new ArrayList<String>();
        techPackList.add(EVENT_E_SGEH);
        techPackList.add(EVENT_E_LTE);
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, techPackList);

        FormattedDateTimeRange newDateTimeRange = dateTimeRange;
        final String drilltype = queryUtils.getDrillType(requestParameters);
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(DRILLTYPE_PARAM, drilltype);

        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        String templateMappingDrillType = null;
        String template;
        if (!drilltype.equals(EVENTS_DRILL_TYPE_PARAM)) {
            newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange, techPackList);
            templateParameters.put(TIMERANGE_PARAM, timerange);

            if (!updateTemplateWithTechpackTables(templateParameters, newDateTimeRange, templateMappingDrillType)) {
                return JSONUtils.JSONEmptySuccessResult();
            }

            if (shouldApplyDataTiering()) {
                templateParameters.put(
                        SUC_TIMERANGE,
                        ViewTypeSelector.returnSuccessAggregateViewType(
                                EventDataSourceType.getEventDataSourceType(timerange), techPackList.get(0)));
                template = getTemplate(KPI_RATIO, requestParameters, templateMappingDrillType, timerange,
                        applicationConfigManager.isDataTieringEnabled());
            } else {
                template = getTemplate(KPI_RATIO, requestParameters, templateMappingDrillType);
            }
        } else {
            templateMappingDrillType = drilltype;

            templateParameters.put(COUNT_PARAM,
                    getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));
            if (!updateTemplateWithRAWTables(templateParameters, newDateTimeRange, KEY_TYPE_ERR, RAW_NON_LTE_TABLES,
                    RAW_LTE_TABLES)) {
                return JSONUtils.JSONEmptySuccessResult();
            }
            timeColumn = EVENT_TIME_COLUMN_INDEX;
            template = getTemplate(KPI_RATIO, requestParameters, templateMappingDrillType);
        }

        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);
        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        if (shouldApplyDataTiering()) {
            newDateTimeRange = dateTimeHelper.getDataTieredDateTimeRange(newDateTimeRange);
        }

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumn, query, newDateTimeRange);
            return null;
        }

        return this.dataService.getGridData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), timeColumn, tzOffset,
                getLoadBalancingPolicy(requestParameters));
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method checks update the templateParameters with techPacktables
     * 
     * @param templateParameters the template parameters
     * @param dateTimeRange the date and time range
     * @param type the type parameter
     * 
     * @return false if no RAW error tables found or no RAW success tables found
     */
    private boolean updateTemplateWithTechpackTables(final Map<String, Object> templateParameters,
            final FormattedDateTimeRange dateTimeRange, final String type) {
        final TechPackTables techPackTablesSGEH = getRawTables(type, dateTimeRange,
                Arrays.asList(new String[] { TechPackData.EVENT_E_SGEH }));
        final TechPackTables techPackTablesLTE = getRawTables(type, dateTimeRange,
                Arrays.asList(new String[] { TechPackData.EVENT_E_LTE }));
        templateParameters.put(TECH_PACK_TABLES_SGEH, techPackTablesSGEH);
        templateParameters.put(TECH_PACK_TABLES_LTE, techPackTablesLTE);

        return isTechPackTablesValid(techPackTablesSGEH) || isTechPackTablesValid(techPackTablesLTE);
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

    /*
     * There's no success information in case of Access Area so data tiering is not applicable
     */
    private boolean shouldApplyDataTiering() {
        if (applicationConfigManager.isDataTieringEnabled()) {
            return true;
        }
        return false;

    }
}