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
 * Sub-resource for event analysis detail and summary
 * request handling.
 *
 * @author edeccox
 * @author ehaoswa
 * @author estepdu
 * @since  Apr 2010
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class EventAnalysisResource extends AbstractResource {

    @EJB(beanName = "EventAnalysisService")
    private Service service;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.PrototypeBaseResource#getService()
     */
    @Override
    protected Service getService() {
        return service;
    }
}
