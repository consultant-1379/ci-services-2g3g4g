/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 * sub resource of Roaming Analysis
 *
 * @author ezhelao
 * @since 01/2012
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class RoamingDrillByCountryResource extends AbstractResource {

    @EJB(beanName = "RoamingDrillByCountryService")
    private Service service;

    @Override
    protected Service getService() {
        return this.service;
    }
}
