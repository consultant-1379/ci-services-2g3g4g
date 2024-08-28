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
public class QOSStatisticsResourceCellWithPreparedTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    public QOSStatisticsResourceCellWithPreparedTablesTest() {
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_ERR_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_DAY);
    }

    @Test
    public void testGetSummary_OneWeek_AccessCell() throws Exception {
        final String node = LTECELL1 + COMMA + "" + COMMA + ERBS1 + COMMA + ERICSSON + COMMA + LTE;
        getQCISummaryForTwoWeeks(TYPE_CELL, node);

    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.ranking.accessarea.BaseQOSStatisticsResourceWithPreparedTables#getTableSpecificValues()
     */
    @Override
    protected Map<String, Object> getTableSpecificColumnsAndValues() {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(RAT, RAT_INTEGER_VALUE_FOR_4G);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, ERBS1);
        values.put(HIERARCHY_1, LTECELL1);
        return values;
    }

}
