/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_APN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_APN;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_APN_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.VARCHAR_127;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.VARCHAR_64;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY;

/**
 * @author eemecoy
 *
 */
public class QOSStatisticsResourceAPNGroupWithPreparedTablesTest extends
        BaseQOSStatisticsResourceWithPreparedTablesTest {

    private static final String GROUP_TYPE_E_APN = "#GROUP_TYPE_E_APN";

    private final HashMap<String, String> apnGroupTableColumns;

    private final HashMap<String, Object> valuesForGroupTable;

    public QOSStatisticsResourceAPNGroupWithPreparedTablesTest() {
        apnGroupTableColumns = new HashMap<String, String>();
        apnGroupTableColumns.put(GROUP_NAME, VARCHAR_64);
        apnGroupTableColumns.put(APN, VARCHAR_127);
        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(APN, SAMPLE_APN);
        valuesForGroupTable.put(GROUP_NAME, SAMPLE_APN_GROUP);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_APN, apnGroupTableColumns);
        insertRow(GROUP_TYPE_E_APN, valuesForGroupTable);
    }

    @Test
    public void testGetSummaryWithDataTieredOn_30MIN_APNGroup() throws Exception {
        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN);
        jndiProperties.setUpDataTieringJNDIProperty();
        getQCIGroupSummary(TYPE_APN, SAMPLE_APN_GROUP, THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummary_OneWeek_APNGroup() throws Exception {
        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY);
        getQCIGroupSummary(TYPE_APN, SAMPLE_APN_GROUP, ONE_WEEK);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.qosstatistics.BaseQOSStatisticsResourceWithPreparedTables#getTableSpecificValues()
     */
    @Override
    protected Map<String, Object> getTableSpecificColumnsAndValues() {
        final Map<String, Object> columnsAndValues = new HashMap<String, Object>();
        columnsAndValues.put(APN, SAMPLE_APN);
        return columnsAndValues;
    }

}
