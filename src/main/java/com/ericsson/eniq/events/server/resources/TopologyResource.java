/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;

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

import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.query.resultsettransformers.ResultSetTransformerFactory;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

/**
 * Resource for performing topology queries
 * 
 * @author eemecoy
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class TopologyResource extends BaseResource {

    private final String[] requiredParametersForNodeParam = new String[] { NODE_PARAM, TYPE_PARAM };

    private final String[] requiredParametersForCellParam = new String[] { TYPE_PARAM };

    private final static String INVALID_CELL_PARAM_VALUE = "Cell value is null or empty";

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#getData(javax.ws.rs.core.MultivaluedMap)
     */
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
        //this method isn't used by this class, it uses the method checkRequiredParametersExist() instead
        throw new UnsupportedOperationException();
    }

    @Path(THREE_G + "/" + CONNECTED_CELLS)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    /**
     * Given a SAC, list all connected cells
     * URL: http://<server>:18080/EniqEventsServices/NETWORK/TOPOLOGY/3G/CONNECTED_CELLS?type=CELL&node=18,RNC01,Ericsson,1
     * 
     * The type in the URL of cell is a bit of a misnomer - it should really be sac, but to change it would mean changing
     * cell everywhere thru the UI and Services code to be the more generic access_area
     */
    public String getListOfConnectedCells() {
        final MultivaluedMap<String, String> requestParameters = getDecodedQueryParameters();
        final String drillType = null;
        /*
         * The query used to fetch topology data uses only "CELL" parameter in where clause.
         * The cell parameter can be provided as
         * Node parameter  "node=598,ONRM_RootMo_R:RNC01:RNC01,Ericsson,3G" where "598" is the CELL value
         * Cell Parameter  "cell=598" i.e "598" is the CELL value
         * The validation should be carried out separately for query containing node parameter
         * and cell parameter
         */
        final String errMsg = validateParameters(requestParameters);
        if (errMsg != null) {
            return errMsg;
        }
        final String query = templateUtils.getQueryFromTemplate(getTemplate(TOPOLOGY, requestParameters, drillType));
        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        //we are only interested in the cell parameter
        String cell = requestParameters.getFirst(CELL_PARAM);

        //if the cell parameter is blank and the type parameter is CELL, then split the node and take cell from there
        if (StringUtils.isBlank(cell)) {
            final String type = requestParameters.getFirst(TYPE_PARAM);
            if (StringUtils.indexOf(type, TYPE_CELL) != -1) {
                final String node = requestParameters.getFirst(NODE_PARAM);
                final String[] value = node.split(DELIMITER);
                cell = value[0];
            }
        }
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(CELL_SQL_NAME, QueryParameter.createStringParameter(cell));

        return this.dataService.getData(CANCEL_REQ_NOT_SUPPORTED, query, queryParameters,
                ResultSetTransformerFactory.getSACConnectedCellsTransformer());
    }

    /*
     * This method is used to validate the parameters where the query contains Node and CELL parameter
     * 
     */
    private String validateParameters(final MultivaluedMap<String, String> requestParameters) {
        String errMgs = null;
        if (requestParameters.containsKey(CELL_PARAM)) {
            errMgs = checkRequiredParametersExistAndReturnErrorMessage(requestParameters,
                    requiredParametersForCellParam);
            if (errMgs == null) {
                final String cellValue = requestParameters.getFirst(CELL_PARAM);
                if (StringUtils.isBlank(cellValue)) {
                    final List<String> errs = new ArrayList<String>();
                    errs.add(INVALID_CELL_PARAM_VALUE);
                    errMgs = getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errs);
                }
            }
        } else {
            errMgs = checkRequiredParametersExistAndReturnErrorMessage(requestParameters,
                    requiredParametersForNodeParam);
            if (errMgs == null) {
                if (!queryUtils.checkValidValue(requestParameters)) {
                    errMgs = JSONUtils.jsonErrorInputMsg();
                }
            }
        }
        return errMgs;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.BaseResource#isValidValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }
}
