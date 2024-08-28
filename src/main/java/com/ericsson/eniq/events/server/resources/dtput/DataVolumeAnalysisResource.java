/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.dtput;

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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.resources.BaseResource;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * Sub-resource for data volume analysis detail request handling. (Data Throughput project)
 *
 * @author evarzol
 * @since  May 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class DataVolumeAnalysisResource extends BaseResource {
    private final static Map<String, String> typeToTPAggregationTableMapping = new HashMap<String, String>();

    @EJB
    private TechPackCXCMappingService techPackCXCMappingService;

    static {
        typeToTPAggregationTableMapping.put(TYPE_APN, TYPE_APN);
        typeToTPAggregationTableMapping.put(TYPE_SGSN, TYPE_SGSN);
        typeToTPAggregationTableMapping.put(TYPE_TAC, TYPE_TERM);
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }

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
        if (requestParameters.containsKey(TYPE_PARAM)) {
            if (!requestParameters.containsKey(GROUP_NAME_PARAM)) {
                final String type = requestParameters.getFirst(TYPE_PARAM);
                if (TYPE_TAC.equals(type) && !requestParameters.containsKey(TAC_PARAM)) {
                    errors.add(TAC_PARAM);
                }
                if (TYPE_APN.equals(type) && !requestParameters.containsKey(APN_PARAM)) {
                    errors.add(APN_PARAM);
                }
                if (TYPE_SGSN.equals(type) && !requestParameters.containsKey(SGSN_PARAM)) {
                    errors.add(SGSN_PARAM);
                }
            }
        } else {
            errors.add(TYPE_PARAM);
        }
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
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)
                || requestParameters.containsKey(IMSI_PARAM) || requestParameters.containsKey(PTMSI_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param requestId
     * @param requestParameters
     * 
     */
    private String getGridResults(final String requestId, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        final String type = requestParameters.getFirst(TYPE_PARAM);
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final String timeColumn = null;
        //  this may be hacky here 
        // needed to handle UTC conversion for csv export
        addDVDrillKey(requestParameters);
        final List<String> techPackList = new ArrayList<String>();
        techPackList.add(EVENT_E_GSN_DT_TPNAME);

        //If techpack not installed return error
        for (final String techPackName : techPackList) {
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
                requestParameters, techPackList);
        removeDVDrillKey(requestParameters);
        final String drillType = null;
        updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);

        final String template = getTemplate(DATAVOLUME_ANALYSIS, requestParameters, drillType);

        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        //put value into templates
        templateParameters.put(TYPE_PARAM, type);
        templateParameters.put(COUNT_PARAM,
                getCountValue(requestParameters, MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE));

        updateTemplateWithRAWTables(templateParameters, dateTimeRange, KEY_TYPE_DT, RAW_DTPDP_TABLES, RAW_DT_TABLES);

        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);
        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, dateTimeRange);

        //  this may be hacky here 
        //	to make sure the UTC conversion can be handled for drill down        
        addDVDrillKey(requestParameters);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, tzOffset, timeColumn, query, dateTimeRange);
            return null;
        }

        return this.dataService.getGridData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, dateTimeRange), timeColumn, tzOffset,
                getLoadBalancingPolicy(requestParameters));

    }

    /**
     * Adds the DV_DRILL_DOWN Key for handling 
     * drill down on  Data volume UL/DL 
     * @param requestParameters     
     */
    private void addDVDrillKey(final MultivaluedMap<String, String> requestParameters) {
        if (!requestParameters.containsKey(KEY_PARAM)) {
            requestParameters.putSingle(KEY_PARAM, KEY_TYPE_DV_DRILL_DOWN);
        }
    }

    /**
     * Removes the DV_DRILL_DOWN Key used for handling 
     * drill down on  Data volume UL/DL
     * so that it has no effect on any other  
     * 
     * @param requestParameters     
     */
    private void removeDVDrillKey(final MultivaluedMap<String, String> requestParameters) {
        final String key = requestParameters.getFirst(KEY_PARAM);
        if (StringUtils.isNotBlank(key) && key.equals(KEY_TYPE_DV_DRILL_DOWN)) {
            requestParameters.remove(KEY_PARAM);
        }
    }
}