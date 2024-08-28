/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dtput;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.dtput.DatavolRoamingAnalysisResource;
import com.ericsson.eniq.events.server.test.queryresults.DataVolumeRoamingResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Ignore;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.CHART_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATE_FROM_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATE_TIME_FORMAT;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATE_TO_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_FROM_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_TO_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_ROAMING_COUNTRY;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_ROAMING_OPERATOR;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_MCCMNC;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_NETWORK_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author evarzol
 * @since July, 2011
 *
 */
@Ignore
public class DataVolumeRoamingWithPreparedTablesTest extends
        TestsWithTemporaryTablesBaseTestCase<DataVolumeRoamingResult> {
    private DatavolRoamingAnalysisResource datavolRoamingAnalysisResource;

    private MultivaluedMap<String, String> map;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String TIME = "30";

    private static final String TIME_FROM = "0000";

    private static final String TIME_TO = "0000";

    private static final String DATE_FROM = "01012000";

    private static final String DATE_TO = "01013000";

    private static final long MEGABYTE = 1024L * 1024L;

    private static final long TEST_MCC_01 = 1;

    private static final long TEST_MNC_01 = 1;

    private static final String TEST_COUNTRY_01 = "Ireland";

    private static final String TEST_OPERATOR_01 = "Vodafone";

    private static final long TEST_DL_01 = 1000L;

    private static final long TEST_UL_01 = 3000L;

    private static final long TEST_ROAMING_01 = 1;

    private static final String TEST_DATETIME_ID_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -41);

    private static final long TEST_MCC_02 = 2;

    private static final long TEST_MNC_02 = 2;

    private static final String TEST_COUNTRY_02 = "Hungary";

    private static final String TEST_OPERATOR_02 = "T-Mobile";

    private static final long TEST_DL_02 = 2000L;

    private static final long TEST_UL_02 = 4000L;

    private static final long TEST_ROAMING_02 = 1;

    private static final String TEST_DATETIME_ID_02 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -42);

    private static final long TEST_DL_03 = 2400L;

    private static final long TEST_UL_03 = 4200L;

    private static final long TEST_ROAMING_03 = 1;

    private static final String TEST_DATETIME_ID_03 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -43);

    private static final long TEST_DL_04 = 2400L;

    private static final long TEST_UL_04 = 4200L;

    private static final long TEST_ROAMING_04 = 0;

    private static final String TEST_DATETIME_ID_04 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -44);

    private final static int EXPECTED_TOTAL_RESULTS = 2;

    private final static long EXPECTED_TOTAL_DL_COUNTRY_01 = TEST_DL_01 + TEST_DL_03;

    private final static long EXPECTED_TOTAL_DL_COUNTRY_02 = TEST_DL_02;

    private final static long EXPECTED_TOTAL_UL_COUNTRY_01 = TEST_UL_01 + TEST_UL_03;

    private final static long EXPECTED_TOTAL_UL_COUNTRY_02 = TEST_UL_02;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_COUNTRY_01 = 2;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_COUNTRY_02 = 1;

    private final static long EXPECTED_TOTAL_DL_OPERATOR_01 = TEST_DL_01;

    private final static long EXPECTED_TOTAL_DL_OPERATOR_02 = TEST_DL_02 + TEST_DL_03;

    private final static long EXPECTED_TOTAL_UL_OPERATOR_01 = TEST_UL_01;

    private final static long EXPECTED_TOTAL_UL_OPERATOR_02 = TEST_UL_02 + TEST_UL_03;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_OPERATOR_01 = 1;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_OPERATOR_02 = 2;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        datavolRoamingAnalysisResource = new DatavolRoamingAnalysisResource();

        final Map<String, String> mccMncColumns = new HashMap<String, String>();
        mccMncColumns.put("MCC", "unsigned bigint");
        mccMncColumns.put("MNC", "unsigned bigint");
        mccMncColumns.put("COUNTRY", "varchar(255)");
        mccMncColumns.put("OPERATOR", "varchar(255)");

        final Map<String, String> dtRawColumns = new HashMap<String, String>();
        dtRawColumns.put("IMSI_MCC", "unsigned bigint");
        dtRawColumns.put("IMSI_MNC", "unsigned bigint");
        dtRawColumns.put("DATAVOL_DL", "unsigned bigint");
        dtRawColumns.put("DATAVOL_UL", "unsigned bigint");
        dtRawColumns.put("ROAMING", "unsigned bigint");
        dtRawColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtColumns = new HashMap<String, String>();
        dtColumns.put("MCC", "unsigned bigint");
        dtColumns.put("MNC", "unsigned bigint");
        dtColumns.put("DATAVOL_DL", "unsigned bigint");
        dtColumns.put("DATAVOL_UL", "unsigned bigint");
        dtColumns.put("ROAMING", "unsigned bigint");
        dtColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtPdpRawColumns = new HashMap<String, String>();
        dtPdpRawColumns.put("IMSI_MCC", "unsigned bigint");
        dtPdpRawColumns.put("IMSI_MNC", "unsigned bigint");
        dtPdpRawColumns.put("DATAVOL_DL", "unsigned bigint");
        dtPdpRawColumns.put("DATAVOL_UL", "unsigned bigint");
        dtPdpRawColumns.put("ROAMING", "unsigned bigint");
        dtPdpRawColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtPdpColumns = new HashMap<String, String>();
        dtPdpColumns.put("MCC", "unsigned bigint");
        dtPdpColumns.put("MNC", "unsigned bigint");
        dtPdpColumns.put("DATAVOL_DL", "unsigned bigint");
        dtPdpColumns.put("DATAVOL_UL", "unsigned bigint");
        dtPdpColumns.put("ROAMING", "unsigned bigint");
        dtPdpColumns.put("NO_OF_TOTAL", "unsigned bigint");
        dtPdpColumns.put("DATETIME_ID", "varchar(255)");

//        createTemporaryTable(TEMP_DIM_E_SGEH_MCCMNC, mccMncColumns);
        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_GSN_DT_RAW, dtRawColumns);
        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_GSN_DTPDP_RAW, dtPdpRawColumns);
        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, dtColumns);
        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, dtPdpColumns);
        populateTemporaryTables();
        attachDependencies(datavolRoamingAnalysisResource);
        map = new MultivaluedMapImpl();
        DummyUriInfoImpl.setUriInfo(map, datavolRoamingAnalysisResource);
    }

    /**
     * @throws SQLException 
     * 
     */
    private void populateTemporaryTables() throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues.put("MCC", TEST_MCC_01);
        tableValues.put("MNC", TEST_MNC_01);
        tableValues.put("COUNTRY", TEST_COUNTRY_01);
        tableValues.put("OPERATOR", TEST_OPERATOR_01);
        insertRow(TEMP_DIM_E_SGEH_MCCMNC, tableValues);
        tableValues.put("MCC", TEST_MCC_02);
        tableValues.put("MNC", TEST_MNC_02);
        tableValues.put("COUNTRY", TEST_COUNTRY_02);
        tableValues.put("OPERATOR", TEST_OPERATOR_02);
        insertRow(TEMP_DIM_E_SGEH_MCCMNC, tableValues);
        tableValues.put("MCC", TEST_MCC_01);
        tableValues.put("MNC", TEST_MNC_02);
        tableValues.put("COUNTRY", TEST_COUNTRY_01);
        tableValues.put("OPERATOR", TEST_OPERATOR_02);
        insertRow(TEMP_DIM_E_SGEH_MCCMNC, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("IMSI_MCC", TEST_MCC_01);
        tableValues.put("IMSI_MNC", TEST_MNC_01);
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_01);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("IMSI_MCC", TEST_MCC_02);
        tableValues.put("IMSI_MNC", TEST_MNC_02);
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_02);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("IMSI_MCC", TEST_MCC_01); // !!!
        tableValues.put("IMSI_MNC", TEST_MNC_02); // !!!
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_03);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("IMSI_MCC", TEST_MCC_01); // !!!
        tableValues.put("IMSI_MNC", TEST_MNC_02); // !!!
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_04);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("MCC", TEST_MCC_01);
        tableValues.put("MNC", TEST_MNC_01);
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_01);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("MCC", TEST_MCC_02);
        tableValues.put("MNC", TEST_MNC_02);
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_02);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("MCC", TEST_MCC_01); // !!!
        tableValues.put("MNC", TEST_MNC_02); // !!!
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_03);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("MCC", TEST_MCC_01); // !!!
        tableValues.put("MNC", TEST_MNC_02); // !!!
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("ROAMING", TEST_ROAMING_04);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
    }

    @Test
    public void testForCountryUsingRawTables() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0000");
        validateResults(datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForOperatorUsingRawTables() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0000");
        validateResults(datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForCountryUsingDailyAggregationTables() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0000");
        validateResults(datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForOperatorUsingDailyAggregationTables() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, "+0000");
        validateResults(datavolRoamingAnalysisResource.getRoamingResults("CANCEL_REQUEST_NOT_SUPPORTED",
                TYPE_ROAMING_OPERATOR, map));
    }

    private void validateResults(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<DataVolumeRoamingResult> dataVolumeRoamingResults = getTranslator().translateResult(jsonResult,
                DataVolumeRoamingResult.class);
        assertThat(dataVolumeRoamingResults.size(), is(EXPECTED_TOTAL_RESULTS));

        for (final DataVolumeRoamingResult dataVolumeRoamingResult : dataVolumeRoamingResults) {
            validateResult(dataVolumeRoamingResult);
        }
    }

    private void validateResult(final DataVolumeRoamingResult dataVolumeRoamingResult) {
        if (TEST_COUNTRY_01.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            assertThat(dataVolumeRoamingResult.getDl(), is(EXPECTED_TOTAL_DL_COUNTRY_01));
            assertThat(dataVolumeRoamingResult.getUl(), is(EXPECTED_TOTAL_UL_COUNTRY_01));
            assertThat(dataVolumeRoamingResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_COUNTRY_01));
        } else if (TEST_COUNTRY_02.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            assertThat(dataVolumeRoamingResult.getDl(), is(EXPECTED_TOTAL_DL_COUNTRY_02));
            assertThat(dataVolumeRoamingResult.getUl(), is(EXPECTED_TOTAL_UL_COUNTRY_02));
            assertThat(dataVolumeRoamingResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_COUNTRY_02));
        } else if (TEST_OPERATOR_01.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            assertThat(dataVolumeRoamingResult.getDl(), is(EXPECTED_TOTAL_DL_OPERATOR_01));
            assertThat(dataVolumeRoamingResult.getUl(), is(EXPECTED_TOTAL_UL_OPERATOR_01));
            assertThat(dataVolumeRoamingResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_OPERATOR_01));
        } else if (TEST_OPERATOR_02.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            assertThat(dataVolumeRoamingResult.getDl(), is(EXPECTED_TOTAL_DL_OPERATOR_02));
            assertThat(dataVolumeRoamingResult.getUl(), is(EXPECTED_TOTAL_UL_OPERATOR_02));
            assertThat(dataVolumeRoamingResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_OPERATOR_02));
        }
    }
}
