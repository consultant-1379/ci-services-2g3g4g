/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.roaming;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.RoamingAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.LookupTechPackPopulator;
import com.ericsson.eniq.events.server.test.queryresults.RoamingAnalysisResult;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Ignore;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.CHART_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DIM_E_SGEH_MCCMNC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.IMSI_MCC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.IMSI_MNC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MCC_FOR_ARGENTINA;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MCC_FOR_NORWAY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MCC_USA;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MNC_FOR_MOVISTAR;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MNC_FOR_TELENOR_NORWAY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MNC_T_MOBILE_USA;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_SUCCESSES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ROAMING;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_MCCMNC;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_MCC_MNC_ROAM_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_MCC_MNC_ROAM_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;

/**
 * @author edivkir
 * @since 2011
 *
 */
@Ignore
public class BaseRoamingResourceWithPreparedTables extends TestsWithTemporaryTablesBaseTestCase<RoamingAnalysisResult> {

    protected RoamingAnalysisResource roamingResource;

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private static List<String> rawTables = new ArrayList<String>();

    private static Collection<String> columnsForAggTable = new ArrayList<String>();

    private static List<String> aggTables = new ArrayList<String>();

    static {
        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(IMSI_MCC);
        columnsForRawTable.add(IMSI_MNC);
        columnsForRawTable.add(ROAMING);
        columnsForRawTable.add(DATETIME_ID);

        aggTables.add(TEMP_EVENT_E_SGEH_MCC_MNC_ROAM_15MIN);
        aggTables.add(TEMP_EVENT_E_LTE_MCC_MNC_ROAM_15MIN);

        columnsForAggTable.add(IMSI_MCC);
        columnsForAggTable.add(IMSI_MNC);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(DATETIME_ID);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        roamingResource = new RoamingAnalysisResource();
        attachDependencies(roamingResource);

        for (final String rawTable : rawTables) {
            createTemporaryTable(rawTable, columnsForRawTable);
        }

        for (final String aggTable : aggTables) {
            createTemporaryTable(aggTable, columnsForAggTable);
        }

        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_MCCMNC);
        new LookupTechPackPopulator().populateLookupTable(connection, TEMP_DIM_E_SGEH_MCCMNC);
    }

    protected void populateTemporaryTables(final String dateTime) throws SQLException {
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, 35472900, "460000000000985", MCC_USA, MNC_T_MOBILE_USA, 1,
                dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, 35840902, "460000000015596", MCC_USA, MNC_T_MOBILE_USA, 1,
                dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, 35596601, "454000000231178", MCC_FOR_ARGENTINA,
                MNC_FOR_MOVISTAR, 1, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, 35596601, "454000000231178", MCC_FOR_ARGENTINA,
                MNC_FOR_MOVISTAR, 1, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, 44926861, "234000000217576", MCC_FOR_NORWAY,
                MNC_FOR_TELENOR_NORWAY, 1, dateTime);
    }

    protected void populateAggData(final String time) throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI_MCC, MCC_USA);
        values.put(IMSI_MNC, MNC_T_MOBILE_USA);
        values.put(NO_OF_ERRORS, "0");
        values.put(NO_OF_SUCCESSES, "5");
        values.put(DATETIME_ID, time);

        for (final String aggTable : aggTables) {
            insertRow(aggTable, values);
        }
    }

    private void insertRowIntoRawTable(final String table, final int tac, final String imsi, final String imsiMCC,
            final String imsiMNC, final int roaming, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(IMSI_MCC, imsiMCC);
        values.put(IMSI_MNC, imsiMNC);
        values.put(ROAMING, roaming);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

    protected MultivaluedMap<String, String> getMap() {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        return map;
    }
}
