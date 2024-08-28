/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.Path;
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
 * The Class CauseCodeAnalysisResource.
 *
 * @author etomcor
 * @author ehaoswa
 * @since May 2010S
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
@SuppressWarnings("PMD.CyclomaticComplexity")
// ehaoswa 24/6/10 uri parameters are complex and many decisions need to be made
public class CauseCodeAnalysisResource extends BaseResource {

    @EJB
    private CauseCodeTablesCCResource causeCodeTablesCCResource;

    @EJB
    private CauseCodeTablesSCCResource causeCodeTablesSCCResource;

    private static List<String> listOfTechPacks = new ArrayList<String>();

    static {
        listOfTechPacks.add(TechPackData.EVENT_E_SGEH);
        listOfTechPacks.add(TechPackData.EVENT_E_LTE);
    }

    /**
     * Constructor for class
     * Initializing the look up maps - cannot have these as static, or initialized in a static block, as they
     * can be initialized from several classes
     */
    public CauseCodeAnalysisResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_SUB_CAUSE_CODE, new AggregationTableInfo(EVNTSRC_CC_SCC,
                EventDataSourceType.AGGREGATED_1MIN, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(APN_CC_SCC, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_BSC, new AggregationTableInfo(VEND_HIER3_CC_SCC,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_CELL, new AggregationTableInfo(VEND_HIER321_CC_SCC));
        aggregationViews.put(TYPE_SGSN, new AggregationTableInfo(EVNTSRC_CC_SCC, EventDataSourceType.AGGREGATED_1MIN,
                EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
    }

    /**
     * Gets the cause code tables resource.
     *
     * @return the cause code tables resource
     */
    @Path(CAUSE_CODE_TABLE_CC)
    public CauseCodeTablesCCResource getcauseCodeTablesCCResource() {
        return this.causeCodeTablesCCResource;
    }

    /**
     * Gets the sub cause code tables resource.
     *
     * @return the cause code tables resource
     */
    @Path(CAUSE_CODE_TABLE_SCC)
    public CauseCodeTablesSCCResource getcauseCodeTablesSCCResource() {
        return this.causeCodeTablesSCCResource;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(jjavax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        //Refactor - as no GRID_PARAM means they get a null pointer exception on the GUI
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

        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets the grid results.
     *
     * @param requestParameters the request parameters
     * @return the grid results - null if requesting CSV
     * @throws WebApplicationException the parse exception
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final String drillType = null;
        final Map<String, Object> templateParameters = new HashMap<String, Object>();
        addTypeParameterIfItExistsToTemplateParameters(requestParameters, templateParameters);
        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }
        String timeColumn = null;
        String key = null;
        if (requestParameters.containsKey(KEY_PARAM)) {
            key = requestParameters.getFirst(KEY_PARAM);
        }

        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(
                requestParameters, listOfTechPacks);

        if (!updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_ERR, RAW_ALL_ERR_TABLES,
                RAW_ERR_TABLES, RAW_LTE_TABLES, RAW_NON_LTE_TABLES)) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        FormattedDateTimeRange newDateTimeRange = dateTimeRange;

        templateParameters.put(KEY_PARAM, key);
        templateParameters.put(COUNT_PARAM,
                getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));

        if ((key != null) && (key.equals(KEY_TYPE_ERR))) {
            timeColumn = EVENT_TIME_COLUMN_INDEX;
        } else {
            // getEventDataSourceTypeForGrid returns DAY/RAW/ONE_MINUTE/FIFTEEN_MINUTES
            // this aggregation might not apply for the selected node, template logic selects this
            final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
            templateParameters.put(TIMERANGE_PARAM, timerange);
            newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange, listOfTechPacks);

            if (dataTieringHandler.appplyLatencyForDataTiering(newDateTimeRange, false, listOfTechPacks,
                    requestParameters)) {
                newDateTimeRange = dateTimeHelper.getDataTieredDateTimeRange(newDateTimeRange);
            }
        }

        final String type;

        if (requestParameters.containsKey(TYPE_PARAM)) {
            type = requestParameters.getFirst(TYPE_PARAM);
        } else {
            type = TYPE_SUB_CAUSE_CODE;
        }

        templateParameters.put(USE_AGGREGATION_TABLES,
                shouldQueryUseAggregationView(type, queryUtils.getEventDataSourceType(dateTimeRange).getValue()));

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, queryUtils
                .getEventDataSourceType(dateTimeRange).getValue(), type, listOfTechPacks);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        templateParameters.put(TECH_PACK_TABLES, techPackTables);

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(CAUSE_CODE_ANALYSIS, requestParameters, drillType), templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumn, query, newDateTimeRange);
            return null;
        }
        return this.dataService.getGridData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), timeColumn, tzOffset);
    }

    private void addTypeParameterIfItExistsToTemplateParameters(final MultivaluedMap<String, String> requestParameters,
            final Map<String, Object> templateParameters) {
        if (requestParameters.containsKey(TYPE_PARAM)) {
            final String type = requestParameters.getFirst(TYPE_PARAM);
            templateParameters.put(TYPE_PARAM, type);
        }
    }
}
