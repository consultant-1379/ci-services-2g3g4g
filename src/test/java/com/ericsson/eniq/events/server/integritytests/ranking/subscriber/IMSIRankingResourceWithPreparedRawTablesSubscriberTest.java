package com.ericsson.eniq.events.server.integritytests.ranking.subscriber;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.test.queryresults.TerminalMostAttachedFailuresDrilldownResult;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.IMSIRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleSubscriberRankingResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;


public class IMSIRankingResourceWithPreparedRawTablesSubscriberTest extends
        BaseDataIntegrityTest<MultipleSubscriberRankingResult> {

    private final String worstIMSI = "123456";

    private final String secondWorstIMSI = "1111";

    private final String thirdWorstIMSI = "9999";

    private final String fourthWorstIMSI = "1234569789";

    private final String IMSIZero = "0";

    private IMSIRankingService imsiRankingService;

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static List<String> imsiRawTables = new ArrayList<String>();

    private static final int SOME_TAC = 123456789;




    protected Mockery mockery = new JUnit4Mockery();
    {
        mockery.setImposteriser(ClassImposteriser.INSTANCE);
    }

    static {
        tempRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        imsiRawTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        imsiRawTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

    }

    @Before
    public void onSetUp() {
        imsiRankingService = new IMSIRankingService();
        attachDependencies(imsiRankingService);
    }

    @Test
    public void testGetRankingData_Subscriber_5Minutes() throws Exception {

        createTemporaryRawTables(tempRawTables);
        createTempTableIMSIRawTables(imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        validateIMSIRankingREsult(json);
    }


    @Test
    public void testGetRankingData_Subscriber_30Minutes() throws Exception {

        createTemporaryRawTables(tempRawTables);
        createTempTableIMSIRawTables(imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        validateIMSIRankingREsult(json);
    }


    @Test
    public void testIMSIRankingWithDataTieringOn30Minutes() throws Exception {

        jndiProperties.setUpDataTieringJNDIProperty();
        createTemporaryRawTables(tempRawTables);
        createTempTableIMSIRawTables(imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        validateIMSIRankingREsultDataTiering(json);
        jndiProperties.setUpJNDIPropertiesForTest();

    }


    private void createTemporaryRawTables(List<String> tables) throws SQLException {
        for(final String table :tables)
        {
            createTemporaryRawTable(table);
        }
    }

    private void createTempTableIMSIRawTables(List<String> tables) throws SQLException {
        for(final String table :tables)
        {
            createTempTableIMSIRaw(table);
        }
    }

    private void validateIMSIRankingREsultDataTiering(final String json) throws Exception {
        final List<MultipleSubscriberRankingResult> rankingResults = getTranslator().translateResult(json,
                MultipleSubscriberRankingResult.class);

    }

    private void validateIMSIRankingREsult(final String json) throws Exception {
        final List<MultipleSubscriberRankingResult> rankingResults = getTranslator().translateResult(json,
                MultipleSubscriberRankingResult.class);
        assertThat("Rankings result size should be 4, but it's " + rankingResults.size(), rankingResults.size(), is(4));

        final MultipleSubscriberRankingResult worstIMSIInRanking = rankingResults.get(0);
        assertThat(worstIMSIInRanking.getIMSI(), is(worstIMSI));
        assertThat(worstIMSIInRanking.getNoErrors(), is(4));
        assertThat(worstIMSIInRanking.getNoSuccesses(), is("1"));

        final MultipleSubscriberRankingResult secondWorstIMSIInRanking = rankingResults.get(1);
        assertThat(secondWorstIMSIInRanking.getIMSI(), is(secondWorstIMSI));
        assertThat(secondWorstIMSIInRanking.getNoErrors(), is(3));
        assertThat(secondWorstIMSIInRanking.getNoSuccesses(), is("2"));

        final MultipleSubscriberRankingResult thirdWorstIMSIInRanking = rankingResults.get(2);
        assertThat(thirdWorstIMSIInRanking.getIMSI(), is(thirdWorstIMSI));
        assertThat(thirdWorstIMSIInRanking.getNoErrors(), is(2));
        assertThat(thirdWorstIMSIInRanking.getNoSuccesses(), is("2"));

        final MultipleSubscriberRankingResult fourthWorstIMSIInRanking = rankingResults.get(3);
        assertThat(fourthWorstIMSIInRanking.getIMSI(), is(fourthWorstIMSI));
        assertThat(fourthWorstIMSIInRanking.getNoErrors(), is(1));
        assertThat(fourthWorstIMSIInRanking.getNoSuccesses(), is("0"));

    }


    private void createTempTableIMSIRaw(final String tableName) throws SQLException
    {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tableName
                    + "(IMSI unsigned bigint, NO_OF_SUCCESSES int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void createTemporaryRawTable(final String tempRawSucTable) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempRawSucTable
                    + "(IMSI unsigned bigint, TAC int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();

            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, worstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, worstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, worstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, worstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, worstIMSI, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, secondWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, secondWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, secondWorstIMSI, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, thirdWorstIMSI, sqlExecutor, dateTime);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, fourthWorstIMSI, sqlExecutor, dateTime);

            //These should be excluded from the result. No IMSIs = 0 should be included
            // in the ranking result.
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, IMSIZero, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, IMSIZero, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, IMSIZero, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, IMSIZero, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, IMSIZero, sqlExecutor, dateTime);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, IMSIZero, sqlExecutor, dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertRow(final String table, final String imsi, final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + imsi + "," + SOME_TAC + ",'" + dateTime + "')");
    }

}
