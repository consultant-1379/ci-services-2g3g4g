/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN;

/**
 * @author edivkir
 * @since 2011
 *
 */
public class QOSStatisticsResourceTerminalGroupWithPreparedRawTablesTest extends
        BaseQOSStatisticsResourceWithPreparedTablesTest {

    private static final String TEMP_GROUP_TYPE_E_TAC = "#GROUP_TYPE_E_TAC";

    private Map<String, Object> valuesForGroupTable;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.qosstatistics.BaseQOSStatisticsResourceWithPreparedTables#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();

        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(GROUP_NAME, "DG_GroupNameTAC_249");
        valuesForGroupTable.put(TAC, SOME_TAC);
    }

    @Test
    public void testGetSummary_FiveMinutes_TACGroup() throws Exception {
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForGroupTable);
        getQCIGroupSummaryForFiveMinutes(TYPE_TAC, "DG_GroupNameTAC_249");
    }

    @Test
    public void testGetSummaryWithDataTiering_30Minutes_TACGroup() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForGroupTable);
        getQCIGroupSummary(TYPE_TAC, "DG_GroupNameTAC_249", THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();
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
