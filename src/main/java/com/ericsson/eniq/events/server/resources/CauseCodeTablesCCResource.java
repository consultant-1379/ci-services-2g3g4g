/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import java.io.IOException;
import java.util.List;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.logging.ServicesLogger;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * @author etomcor
 * @since June 2010
 * 
 * Cause codes (CC) tables resource
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class CauseCodeTablesCCResource extends BaseResource {

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters) {
        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(GRID_PARAM)) {
            return getGridData(requestId, requestParameters);
        }

        return getNoSuchDisplayErrorResponse(displayType);
    }

    /*
     * 
     * @param requestId
     * @param requestParameters
     * @return
     */
    private String getGridData(final String requestId, final MultivaluedMap<String, String> requestParameters) {
        final String drillType = null;

        final String query = templateUtils.getQueryFromTemplate(
                getTemplate(CAUSE_CODE_TABLE_CC, requestParameters, drillType), null);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.createJSONErrorResult("Failed to build query");
        }
        if (isMediaTypeApplicationCSV()) {
            try {
                this.streamingDataService.streamDataAsCsv(query, null, null, response.getOutputStream());
            } catch (final IOException e) {
                ServicesLogger.error(getClass().getName(), "getGridData", e);
            }
            return null;
        }
        return this.dataService.getGridData(requestId, query, null, null);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#checkParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }

}
