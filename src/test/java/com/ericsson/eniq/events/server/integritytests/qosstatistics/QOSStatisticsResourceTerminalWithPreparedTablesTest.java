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
 * @author edivkir
 * @since 2011
 *
 */
public class QOSStatisticsResourceTerminalWithPreparedTablesTest extends
        BaseQOSStatisticsResourceWithPreparedTablesTest {

    public QOSStatisticsResourceTerminalWithPreparedTablesTest() {
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY);
    }

    @Test
    public void testGetSummary_OneWeek_AccessCell() throws Exception {
        getQCISummaryForTwoWeeks(TYPE_TAC, "Veriphon D 45-1," + SOME_TAC);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.ranking.accessarea.BaseQOSStatisticsResourceWithPreparedTables#getTableSpecificValues()
     */
    @Override
    protected Map<String, Object> getTableSpecificColumnsAndValues() {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SOME_TAC);
        return values;
    }
}
