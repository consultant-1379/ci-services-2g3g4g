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
public class QOSStatisticsResourceAPNWithRawPreparedTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    public QOSStatisticsResourceAPNWithRawPreparedTablesTest() {
        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN);
    }

    @Test
    public void testGetSummary_FiveMinutes_APN() throws Exception {
        getQCISummary(TYPE_APN, SAMPLE_APN, FIVE_MINUTES);
    }

    @Test
    public void testAPNQoSSummaryWithDataTieringOn30Min() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        getQCISummary(TYPE_APN, SAMPLE_APN, THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.ranking.accessarea.BaseQOSStatisticsResourceWithPreparedTables#getTableSpecificValues()
     */
    @Override
    protected Map<String, Object> getTableSpecificColumnsAndValues() {
        final Map<String, Object> columnsAndValues = new HashMap<String, Object>();
        columnsAndValues.put(APN, SAMPLE_APN);
        return columnsAndValues;
    }

}
