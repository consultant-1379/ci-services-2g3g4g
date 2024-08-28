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
public class QOSStatisticsResourceMMEWithPreparedTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    public QOSStatisticsResourceMMEWithPreparedTablesTest() {

        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY);
    }

    @Test
    public void testGetSummary_OneWeek_MME() throws Exception {
        getQCISummaryForTwoWeeks(TYPE_SGSN, MME1);

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
