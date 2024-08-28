/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.resources;

import javax.ejb.*;

import com.ericsson.eniq.events.server.serviceprovider.Service;

@Stateless
@LocalBean
public class IMSIRankingResource extends AbstractResource {

	private static final String IMSI_RANKING_SERVICE = "IMSIRankingService";

	@EJB(beanName = IMSI_RANKING_SERVICE)
	private Service service;

	@Override
	protected Service getService() {
		return service;
	}

}
