/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.roaminganalysis;


import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.test.queryresults.network.RoamingDrillQuerybyResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 * @author ezhelao
 * @since 01/2012
 */
public class RoamingDrillByCountryRawTest extends
        BaseDataIntegrityTest<RoamingDrillQuerybyResult> {



    private static final String  ROAMING="ROAMING";
    private static final String  IMSI="IMSI";
    private static final String  IMSI_MCC="IMSI_MCC";
    private static final String  IMSI_MNC="IMSI_MNC";
    private static final String  TEMP_EVENT_E_SGEH_ERR_RAW="#EVENT_E_SGEH_ERR_RAW";
    private static final String  TEMP_DIM_E_SGEH_MCCMNC="#DIM_E_SGEH_MCCMNC";
    private static final String  TEMP_DIM_E_SGEH_EVENTTYPE="#DIM_E_SGEH_EVENTTYPE";
    private static final String  TAC="TAC";
    private static final String  SAMPLE_TAC_NUM="125631";
    private static final String  NORWAY_MCC="242";
    private static final String  USA_MCC="310";



    RoamingDrillByCountryService  roamingDrillByCountryService;
    private static final String TEMP_EVENT_E_LTE_ERR_RAW ="#EVENT_E_LTE_ERR_RAW" ;


    @Before
    public void setup() throws Exception {
        roamingDrillByCountryService = new RoamingDrillByCountryService();
        attachDependencies(roamingDrillByCountryService);
        createEventRawErrTable();

        seedTacTable();
        insertAllRawData();

        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_MCCMNC);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_EVENTTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_LTE_EVENTTYPE);

        createAndPopulateTempLookupTable(DIM_E_SGEH_MCCMNC);
        createAndPopulateTempLookupTable(DIM_E_SGEH_EVENTTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_EVENTTYPE);
    }

    @Test
    public  void testFiveMinuteQuery () throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET,TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(DISPLAY_PARAM, CHART_PARAM);
        requestParameters.add(MCC_PARAM, "242");
        requestParameters.add(COUNTRY, "Norway");
        requestParameters.add(MAX_ROWS, DEFAULT_MAX_ROWS);
        List<RoamingDrillQuerybyResult> results = runQueryAssertJSONStringTransform(roamingDrillByCountryService, requestParameters);
        verifyResult(results);


    }

    private void verifyResult(List<RoamingDrillQuerybyResult> results) {
        assertThat(results.size(), is(3));
        assertThat(results.get(0).getEventId(),is("1"));
        assertThat(results.get(0).getImpactedSubscriber(),is("3"));
        assertThat(results.get(0).getNumberOfFailures(),is("4"));
        assertThat(results.get(0).getCountryOperatorName(),is("Norway"));
        assertThat(results.get(0).getEventIdDesc(),is("ACTIVATE"));

        assertThat(results.get(1).getEventId(),is("2"));
        assertThat(results.get(1).getImpactedSubscriber(),is("4"));
        assertThat(results.get(1).getNumberOfFailures(),is("4"));
        assertThat(results.get(1).getCountryOperatorName(),is("Norway"));
        assertThat(results.get(1).getEventIdDesc(),is("RAU"));

        assertThat(results.get(2).getEventId(),is("7"));
        assertThat(results.get(2).getImpactedSubscriber(),is("2"));
        assertThat(results.get(2).getNumberOfFailures(),is("2"));
        assertThat(results.get(2).getCountryOperatorName(),is("Norway"));
        assertThat(results.get(2).getEventIdDesc(),is("L_HANDOVER"));


    }


    private void insertAllRawData() throws SQLException {
        String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();

        insertSgehRawDataRow(NORWAY_MCC, "02", "1", "001", dateTime, SAMPLE_TAC_NUM, "1");
        insertSgehRawDataRow(NORWAY_MCC, "02", "0", "000", dateTime, SAMPLE_TAC_NUM, "1");
        insertSgehRawDataRow(NORWAY_MCC, "02", "1", "002", dateTime, SAMPLE_TAC_NUM, "1");
        insertSgehRawDataRow(NORWAY_MCC, "02", "1", "003", dateTime, SAMPLE_TAC_NUM, "1");
        insertSgehRawDataRow(NORWAY_MCC, "02", "1", "003", dateTime, SAMPLE_TAC_NUM, "1");


        insertSgehRawDataRow(NORWAY_MCC, "03", "1", "001", dateTime, SAMPLE_TAC_NUM, "2");
        insertSgehRawDataRow(NORWAY_MCC, "03", "0", "001", dateTime, SAMPLE_TAC_NUM, "2");
        insertSgehRawDataRow(NORWAY_MCC, "03", "1", "002", dateTime, SAMPLE_TAC_NUM, "2");
        insertSgehRawDataRow(NORWAY_MCC, "03", "0", "003", dateTime, SAMPLE_TAC_NUM, "2");
        insertSgehRawDataRow(NORWAY_MCC, "03", "1", "004", dateTime, SAMPLE_TAC_NUM, "2");

        insertSgehRawDataRow(NORWAY_MCC, "03", "0", "003", dateTime, SAMPLE_TAC_NUM, "2");
        insertSgehRawDataRow(NORWAY_MCC, "03", "1", "003", dateTime, SAMPLE_TAC_NUM, "2");

        insertLteRawDataRow(NORWAY_MCC, "03", "1", "004", dateTime, SAMPLE_TAC_NUM, "7");

        insertLteRawDataRow(NORWAY_MCC, "03", "0", "003", dateTime, SAMPLE_TAC_NUM, "7");
        insertLteRawDataRow(NORWAY_MCC, "04", "1", "003", dateTime, SAMPLE_TAC_NUM, "7");


        insertSgehRawDataRow(USA_MCC, "02", "1", "001", dateTime, SAMPLE_TAC_NUM, "3");
        insertSgehRawDataRow(USA_MCC, "02", "1", "002", dateTime, SAMPLE_TAC_NUM, "2");



    }

    private void insertSgehRawDataRow(String imsi_mcc, String imsi_mnc, String roaming, String imsi, final String time, String tac, String eventId) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI_MCC,imsi_mcc);
        valuesForTable.put(IMSI_MNC, imsi_mnc);
        valuesForTable.put(ROAMING,roaming);
        valuesForTable.put(IMSI,imsi);
        valuesForTable.put(DATETIME_ID,time);
        valuesForTable.put(TAC,tac);
        valuesForTable.put(EVENT_ID,eventId);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, valuesForTable);

    }

     private void insertLteRawDataRow(String imsi_mcc, String imsi_mnc, String roaming, String imsi, final String time, String tac, String eventId) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI_MCC,imsi_mcc);
        valuesForTable.put(IMSI_MNC, imsi_mnc);
        valuesForTable.put(ROAMING,roaming);
        valuesForTable.put(IMSI,imsi);
        valuesForTable.put(DATETIME_ID,time);
        valuesForTable.put(TAC,tac);
        valuesForTable.put(EVENT_ID,eventId);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForTable);

    }



    private void createEventRawErrTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(IMSI_MNC);
        columnsForEventTable.add(ROAMING);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(EVENT_ID);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForEventTable);

        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(IMSI_MNC);
        columnsForEventTable.add(ROAMING);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(EVENT_ID);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForEventTable);


    }


    private void seedTacTable () throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();

        valuesForTable.clear();
        valuesForTable.put(TAC, TEST_VALUE_EXCLUSIVE_TAC);
        valuesForTable.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP_NAME);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTable);

    }



}
