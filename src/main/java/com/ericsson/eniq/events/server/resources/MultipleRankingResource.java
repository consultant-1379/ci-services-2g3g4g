/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 *  
 * This service returns ranking data for the UI widgets from Sybase IQ.
 * 
 *
 * @author eemecoy 
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class MultipleRankingResource extends AbstractResource {

    @EJB(beanName = "MultipleRankingService")
    private Service service;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.PrototypeBaseResource#getService()
     */
    @Override
    protected Service getService() {
        return service;
    }

}
