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
import com.ericsson.eniq.events.server.resources.dtput.NetworkDataVolumeResource;
import com.ericsson.eniq.events.server.test.queryresults.NetworkDataVolumeResult;
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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GROUP_NAME_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.NODE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_FROM_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_TO_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_APN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_SGSN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_APN_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_APN_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_NETWORK_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSN_DT_SGSN_DAY;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_APN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_EVNTSRC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author evarzol
 * @since July, 2011
 *
 */
@Ignore
public class NetworkDataVolumeWithPreparedTablesTest extends
        TestsWithTemporaryTablesBaseTestCase<NetworkDataVolumeResult> {
    private MultivaluedMap<String, String> map;

    private NetworkDataVolumeResource networkDataVolumeResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String TIME = "30";

    private static final String TIME_FROM = "0000";

    private static final String TIME_TO = "0000";

    private static final String DATE_FROM = "01012000";

    private static final String DATE_TO = "01013000";

    private static final long MEGABYTE = 1024L * 1024L;

    private static final String APN1 = "apn1";

    private static final String APN2 = "apn2";

    private static final String APN3 = "apn3";

    private static final String APN_GROUP = "APN Group 1";

    private static final String SGSN1 = "sgsn1";

    private static final String SGSN2 = "sgsn2";

    private static final String SGSN3 = "sgsn3";

    private static final String SGSN_GROUP = "SGSN Group 1";

    // Rows:

    private static final String TEST_APN_01 = APN1;

    private static final String TEST_SGSN_01 = SGSN1;

    private static final long TEST_DL_01 = 1000L;

    private static final long TEST_UL_01 = 3000L;

    private static final String TEST_DATETIME_ID_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -41);

    private static final String TEST_APN_02 = APN1;

    private static final String TEST_SGSN_02 = SGSN1;

    private static final long TEST_DL_02 = 2000L;

    private static final long TEST_UL_02 = 4000L;

    private static final String TEST_DATETIME_ID_02 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -42);

    private static final String TEST_APN_03 = APN2;

    private static final String TEST_SGSN_03 = SGSN2;

    private static final long TEST_DL_03 = 2400L;

    private static final long TEST_UL_03 = 4200L;

    private static final String TEST_DATETIME_ID_03 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -43);

    private static final String TEST_APN_04 = APN2;

    private static final String TEST_SGSN_04 = SGSN2;

    private static final long TEST_DL_04 = 2400L;

    private static final long TEST_UL_04 = 4200L;

    private static final String TEST_DATETIME_ID_04 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -44);

    private static final String TEST_APN_05 = APN3;

    private static final String TEST_SGSN_05 = SGSN3;

    private static final long TEST_DL_05 = 2400L;

    private static final long TEST_UL_05 = 4200L;

    private static final String TEST_DATETIME_ID_05 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE,
            -45);

    // Expected results:

    private final static int EXPECTED_TOTAL_RESULTS = 2;

    private final static long EXPECTED_TOTAL_DL_APN1 = TEST_DL_01 + TEST_DL_02;

    private final static long EXPECTED_TOTAL_UL_APN1 = TEST_UL_01 + TEST_UL_02;

    private final static long EXPECTED_TOTAL_DL_APN2 = TEST_DL_03 + TEST_DL_04;

    private final static long EXPECTED_TOTAL_UL_APN2 = TEST_UL_03 + TEST_UL_04;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_APN1 = 2;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_APN2 = 2;

    private final static long EXPECTED_TOTAL_DL_SGSN1 = TEST_DL_01 + TEST_DL_02;

    private final static long EXPECTED_TOTAL_UL_SGSN1 = TEST_UL_01 + TEST_UL_02;

    private final static long EXPECTED_TOTAL_DL_SGSN2 = TEST_DL_03 + TEST_DL_04;

    private final static long EXPECTED_TOTAL_UL_SGSN2 = TEST_UL_03 + TEST_UL_04;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_SGSN1 = 2;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_SGSN2 = 2;

    private final static long EXPECTED_TOTAL_DL_APN_GROUP = TEST_DL_01 + TEST_DL_02 + TEST_DL_03 + TEST_DL_04;

    private final static long EXPECTED_TOTAL_UL_APN_GROUP = TEST_UL_01 + TEST_UL_02 + TEST_UL_03 + TEST_UL_04;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_APN_GROUP = 4;

    private final static long EXPECTED_TOTAL_DL_SGSN_GROUP = TEST_DL_01 + TEST_DL_02 + TEST_DL_03 + TEST_DL_04;

    private final static long EXPECTED_TOTAL_UL_SGSN_GROUP = TEST_UL_01 + TEST_UL_02 + TEST_UL_03 + TEST_UL_04;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS_SGSN_GROUP = 4;

    private final static long EXPECTED_TOTAL_DL = TEST_DL_01 + TEST_DL_02 + TEST_DL_03 + TEST_DL_04 + TEST_DL_05;

    private final static long EXPECTED_TOTAL_UL = TEST_UL_01 + TEST_UL_02 + TEST_UL_03 + TEST_UL_04 + TEST_DL_05;

    private final static long EXPECTED_TOTAL_PDP_SESSIONS = 5;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        networkDataVolumeResource = new NetworkDataVolumeResource();

        final Map<String, String> apnGroupColumns = new HashMap<String, String>();
        apnGroupColumns.put("APN", "varchar(255)");
        apnGroupColumns.put("GROUP_NAME", "varchar(255)");

        final Map<String, String> sgsnGroupColumns = new HashMap<String, String>();
        sgsnGroupColumns.put("SGSN", "varchar(255)");
        sgsnGroupColumns.put("GROUP_NAME", "varchar(255)");

        final Map<String, String> dtRawColumns = new HashMap<String, String>();
        dtRawColumns.put("DATAVOL_DL", "unsigned bigint");
        dtRawColumns.put("DATAVOL_UL", "unsigned bigint");
        dtRawColumns.put("APN", "varchar(255)");
        dtRawColumns.put("SGSN_NAME", "varchar(255)");
        dtRawColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtAPNColumns = new HashMap<String, String>();
        dtAPNColumns.put("DATAVOL_DL", "unsigned bigint");
        dtAPNColumns.put("DATAVOL_UL", "unsigned bigint");
        dtRawColumns.put("APN", "varchar(255)");
        dtAPNColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtSGSNColumns = new HashMap<String, String>();
        dtSGSNColumns.put("DATAVOL_DL", "unsigned bigint");
        dtSGSNColumns.put("DATAVOL_UL", "unsigned bigint");
        dtSGSNColumns.put("SGSN_NAME", "varchar(255)");
        dtSGSNColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtColumns = new HashMap<String, String>();
        dtColumns.put("DATAVOL_DL", "unsigned bigint");
        dtColumns.put("DATAVOL_UL", "unsigned bigint");
        dtColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtPdpRawColumns = new HashMap<String, String>();
        dtPdpRawColumns.put("DATAVOL_DL", "unsigned bigint");
        dtPdpRawColumns.put("DATAVOL_UL", "unsigned bigint");
        dtRawColumns.put("APN", "varchar(255)");
        dtRawColumns.put("SGSN_NAME_1", "varchar(255)");
        dtRawColumns.put("SGSN_NAME_2", "varchar(255)");
        dtRawColumns.put("SGSN_NAME_3", "varchar(255)");
        dtRawColumns.put("SGSN_NAME_4", "varchar(255)");
        dtRawColumns.put("SGSN_NAME_5", "varchar(255)");
        dtPdpRawColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtPdpAPNColumns = new HashMap<String, String>();
        dtPdpAPNColumns.put("DATAVOL_DL", "unsigned bigint");
        dtPdpAPNColumns.put("DATAVOL_UL", "unsigned bigint");
        dtPdpAPNColumns.put("NO_OF_TOTAL", "unsigned bigint");
        dtPdpAPNColumns.put("APN", "varchar(255)");
        dtPdpAPNColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtPdpSGSNColumns = new HashMap<String, String>();
        dtPdpSGSNColumns.put("DATAVOL_DL", "unsigned bigint");
        dtPdpSGSNColumns.put("DATAVOL_UL", "unsigned bigint");
        dtPdpSGSNColumns.put("NO_OF_TOTAL", "unsigned bigint");
        dtPdpSGSNColumns.put("SGSN_NAME", "varchar(255)");
        dtPdpSGSNColumns.put("DATETIME_ID", "varchar(255)");

        final Map<String, String> dtPdpColumns = new HashMap<String, String>();
        dtPdpColumns.put("DATAVOL_DL", "unsigned bigint");
        dtPdpColumns.put("DATAVOL_UL", "unsigned bigint");
        dtPdpColumns.put("NO_OF_TOTAL", "unsigned bigint");
        dtPdpColumns.put("DATETIME_ID", "varchar(255)");

        createTemporaryTable(TEMP_GROUP_TYPE_E_APN, apnGroupColumns);
        createTemporaryTable(TEMP_GROUP_TYPE_E_EVNTSRC, sgsnGroupColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_RAW, dtRawColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_RAW, dtPdpRawColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_APN_DAY, dtAPNColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_APN_DAY, dtPdpAPNColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_SGSN_DAY, dtSGSNColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY, dtPdpSGSNColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, dtColumns);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, dtPdpColumns);
        populateTemporaryTables();
        attachDependencies(networkDataVolumeResource);
        map = new MultivaluedMapImpl();
        DummyUriInfoImpl.setUriInfo(map, networkDataVolumeResource);
    }

    /**
     * @throws SQLException 
     * 
     */
    private void populateTemporaryTables() throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues.put("APN", APN1);
        tableValues.put("GROUP_NAME", APN_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_APN, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", APN2);
        tableValues.put("GROUP_NAME", APN_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_APN, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN", SGSN1);
        tableValues.put("GROUP_NAME", SGSN_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_EVNTSRC, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN", SGSN2);
        tableValues.put("GROUP_NAME", SGSN_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_EVNTSRC, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_01);
        tableValues.put("SGSN_NAME", TEST_SGSN_01);
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        tableValues.put("APN", TEST_APN_02);
        tableValues.put("SGSN_NAME", TEST_SGSN_02);
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        tableValues.put("APN", TEST_APN_03);
        tableValues.put("SGSN_NAME", TEST_SGSN_03);
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        tableValues.put("APN", TEST_APN_04);
        tableValues.put("SGSN_NAME", TEST_SGSN_04);
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);
        tableValues.put("APN", TEST_APN_05);
        tableValues.put("SGSN_NAME", TEST_SGSN_05);
        tableValues.put("DATAVOL_DL", TEST_DL_05 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_05 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_05);
        insertRow(TEMP_EVENT_E_GSN_DT_RAW, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_01);
        tableValues.put("SGSN_NAME_01", TEST_SGSN_01);
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("APN", TEST_APN_02);
        tableValues.put("SGSN_NAME_01", TEST_SGSN_02);
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("APN", TEST_APN_03);
        tableValues.put("SGSN_NAME_01", TEST_SGSN_03);
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("APN", TEST_APN_04);
        tableValues.put("SGSN_NAME_01", TEST_SGSN_04);
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);
        tableValues.put("APN", TEST_APN_05);
        tableValues.put("SGSN_NAME_01", TEST_SGSN_05);
        tableValues.put("DATAVOL_DL", TEST_DL_05 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_05 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_05);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_RAW, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_01);
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DT_APN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_APN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_02);
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DT_APN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_APN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_03);
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DT_APN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_APN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_04);
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DT_APN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_APN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("APN", TEST_APN_05);
        tableValues.put("DATAVOL_DL", TEST_DL_05 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_05 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_05);
        insertRow(TEMP_EVENT_E_GSN_DT_APN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_APN_DAY, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN_NAME", TEST_SGSN_01);
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DT_SGSN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN_NAME", TEST_SGSN_02);
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DT_SGSN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN_NAME", TEST_SGSN_03);
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DT_SGSN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN_NAME", TEST_SGSN_04);
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DT_SGSN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("SGSN_NAME", TEST_SGSN_05);
        tableValues.put("DATAVOL_DL", TEST_DL_05 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_05 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_05);
        insertRow(TEMP_EVENT_E_GSN_DT_SGSN_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_SGSN_DAY, tableValues);

        tableValues = new HashMap<String, Object>();
        tableValues.put("DATAVOL_DL", TEST_DL_01 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_01 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_01);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("DATAVOL_DL", TEST_DL_02 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_02 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_02);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("DATAVOL_DL", TEST_DL_03 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_03 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_03);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("DATAVOL_DL", TEST_DL_04 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_04 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_04);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
        tableValues = new HashMap<String, Object>();
        tableValues.put("DATAVOL_DL", TEST_DL_05 * MEGABYTE);
        tableValues.put("DATAVOL_UL", TEST_UL_05 * MEGABYTE);
        tableValues.put("DATETIME_ID", TEST_DATETIME_ID_05);
        insertRow(TEMP_EVENT_E_GSN_DT_NETWORK_DAY, tableValues);
        tableValues.put("NO_OF_TOTAL", 1);
        insertRow(TEMP_EVENT_E_GSN_DTPDP_NETWORK_DAY, tableValues);
    }

    private void validateResults(final String jsonResult, final String toTest) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<NetworkDataVolumeResult> networkDataVolumeResults = getTranslator().translateResult(jsonResult,
                NetworkDataVolumeResult.class);
        assertThat(networkDataVolumeResults.size(), is(EXPECTED_TOTAL_RESULTS));

        for (final NetworkDataVolumeResult networkDataVolumeResult : networkDataVolumeResults) {
            validateResult(networkDataVolumeResult, toTest);
        }
    }

    private void validateResult(final NetworkDataVolumeResult networkDataVolumeResult, final String toTest) {
        if (APN1.equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL_APN1));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL_APN1));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_APN1));
        } else if (APN2.equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL_APN2));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL_APN2));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_APN2));
        } else if (SGSN1.equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL_SGSN1));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL_SGSN1));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_SGSN1));
        } else if (SGSN2.equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL_SGSN2));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL_SGSN2));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_SGSN2));
        } else if (APN_GROUP.equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL_APN_GROUP));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL_APN_GROUP));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_APN_GROUP));
        } else if (SGSN_GROUP.equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL_SGSN_GROUP));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL_SGSN_GROUP));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS_SGSN_GROUP));
        } else if ("".equals(toTest)) {
            assertThat(networkDataVolumeResult.getDl(), is(EXPECTED_TOTAL_DL));
            assertThat(networkDataVolumeResult.getUl(), is(EXPECTED_TOTAL_UL));
            assertThat(networkDataVolumeResult.getPdpSessions(), is(EXPECTED_TOTAL_PDP_SESSIONS));
        }
    }

    @Test
    public void testDrillDownByAPN() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(NODE_PARAM, "blackberry.net");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map),
                APN1);
    }

    @Test
    public void testDrillDownBySGSN() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map),
                SGSN1);
    }

    public void testDrillDownByNetwork() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, TIME);
        map.putSingle(TZ_OFFSET, "+0100");
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map), "");
    }

    @Test
    public void testDrillDownByAPNAggregation() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(NODE_PARAM, "blackberry.net");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map),
                APN2);
    }

    @Test
    public void testDrillDownBySGSNAggregation() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(NODE_PARAM, "SGSN1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map),
                SGSN2);
    }

    public void testDrillDownByNetworkAggregation() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map), "");
    }

    @Test
    public void testGetDetailedDataByAPNGroup() throws Exception {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameAPN_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map),
                APN_GROUP);
    }

    @Test
    public void testGetDetailedDataBySGSNGroup() throws Exception {
        map.clear();
        addTimeParameterToParameterMap();
        map.putSingle(GROUP_NAME_PARAM, "DG_GroupNameEVENTSRC_1");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        validateResults(networkDataVolumeResource.getNetworkDataVolumeResults("CANCEL_REQUEST_NOT_SUPPORTED", map),
                SGSN_GROUP);
    }

    private void addTimeParameterToParameterMap() {
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
    }
}
