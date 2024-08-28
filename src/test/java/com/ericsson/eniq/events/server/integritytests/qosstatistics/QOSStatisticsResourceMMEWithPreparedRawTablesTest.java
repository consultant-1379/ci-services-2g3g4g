/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author eemecoy
 *
 */
public class QOSStatisticsResourceMMEWithPreparedRawTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    @Test
    public void testGetSummary_FiveMinutes_MME() throws Exception {
        getQCISummary(TYPE_SGSN, MME1, FIVE_MINUTES);

    }

    @Test
    public void testGetSummaryWithDataTiering_30Minutes_MME() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN);
        getQCISummary(TYPE_SGSN, MME1, THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();

    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.ranking.accessarea.BaseQOSStatisticsResourceWithPreparedTables#getTableSpecificValues()
     */
    @Override
    protected Map<String, Object> getTableSpecificColumnsAndValues() {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_SOURCE_NAME, MME1);
        return values;
    }

}
