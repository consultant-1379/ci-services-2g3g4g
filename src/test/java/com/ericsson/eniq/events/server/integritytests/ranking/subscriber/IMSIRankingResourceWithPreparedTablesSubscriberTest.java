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


public class IMSIRankingResourceWithPreparedTablesSubscriberTest extends
        BaseDataIntegrityTest<MultipleSubscriberRankingResult> {

    private final static List<String> tempIMSIRankingTables = new ArrayList<String>();

    private final int noErrorsForWorstImsiInLTENetwork = 11;

    private final String worstIMSI = "123456";

    private final String secondWorstIMSI = "12121212";

    private final int noErrorsForSecondWorstImsi = 7;

    private final int noErrorsForWorstImsiIn2G3GNetworks = 4;

    private final String thirdWorstIMSI = "654654";

    private final int noErrorsForThirdWorstImsi = 6;

    private final String fourthWorstIMSI = "99999";

    private final int noErrorsForFourthWorstImsiIn2G3GNetworks = 2;

    private final int noErrorsForFourthWorstImsiInLTENetworks = 1;

    private final String IMSIZero = "0";

    private final int noErrorsForImsiZeroIn2G3GNetworks = 20;

    private final int noErrorsForImsiZeroInLTENetworks = 12;

    private IMSIRankingService imsiRankingService;

    private final static List<String> tempSucRawTables = new ArrayList<String>();

    private final static List<String> imsiRawTables = new ArrayList<String>();

    private static final int SOME_TAC = 123456789;

    private static final String LTE_IMSI_RANK = "#EVENT_E_LTE_IMSI_RANK_DAY";

    private static final String SGEH_IMSI_RANK = "#EVENT_E_SGEH_IMSI_RANK_DAY";




    static {
        tempIMSIRankingTables.add(TEMP_EVENT_E_SGEH_IMSI_RANK);
        tempIMSIRankingTables.add(TEMP_EVENT_E_LTE_IMSI_RANK);
        tempIMSIRankingTables.add(LTE_IMSI_RANK);
        tempIMSIRankingTables.add(SGEH_IMSI_RANK);



        tempSucRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempSucRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        imsiRawTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        imsiRawTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);


    }

    protected Mockery mockery = new JUnit4Mockery();
    {
        mockery.setImposteriser(ClassImposteriser.INSTANCE);
    }


    @Before
    public void onSetUp() {
        imsiRankingService = new IMSIRankingService();
        attachDependencies(imsiRankingService);

    }

    @Test
    public void testGetRankingData_Subscriber_TwoWeeks() throws Exception {
        createTemporaryIMSIRankingTables(tempIMSIRankingTables);
        createTemporaryRawSucTables(tempSucRawTables);
        createTemporaryIMSIRawSucTables (imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        System.out.println(json);
        validateIMSIRankingREsult(json);

    }

    private void  createTemporaryIMSIRankingTables(List<String> tables) throws SQLException {
        for(final String table :tables)
        {
             createTemporaryIMSIRankingTable(table);
        }
    }

    private void createTemporaryRawSucTables(List<String> tables) throws SQLException {
        for(final String table :tables)
        {
            createTemporaryRawSucTable(table);
        }
    }

    private  void createTemporaryIMSIRawSucTables(List<String> tables)   throws SQLException
    {
        for(final String table :tables)
        {
            createTemporaryIMSIRankingSucTable(table);
        }
    }


    private void validateIMSIRankingREsult(final String json) throws Exception {
        final List<MultipleSubscriberRankingResult> rankingResults = getTranslator().translateResult(json,
                MultipleSubscriberRankingResult.class);

        assertThat(rankingResults.size(), is(4));

        final MultipleSubscriberRankingResult worstIMSIInRanking = rankingResults.get(0);
        assertThat(worstIMSIInRanking.getIMSI(), is(worstIMSI));
        assertThat(worstIMSIInRanking.getNoErrors(), is(noErrorsForWorstImsiIn2G3GNetworks
                + noErrorsForWorstImsiInLTENetwork));
        assertThat(worstIMSIInRanking.getNoSuccesses(), is("0"));

        final MultipleSubscriberRankingResult secondWorstIMSIInRanking = rankingResults.get(1);
        assertThat(secondWorstIMSIInRanking.getIMSI(), is(secondWorstIMSI));
        assertThat(secondWorstIMSIInRanking.getNoErrors(), is(noErrorsForSecondWorstImsi));
        assertThat(secondWorstIMSIInRanking.getNoSuccesses(), is("0"));

        final MultipleSubscriberRankingResult thirdWorstIMSIInRanking = rankingResults.get(2);
        assertThat(thirdWorstIMSIInRanking.getIMSI(), is(thirdWorstIMSI));
        assertThat(thirdWorstIMSIInRanking.getNoErrors(), is(noErrorsForThirdWorstImsi));
        assertThat(thirdWorstIMSIInRanking.getNoSuccesses(), is("0"));

        final MultipleSubscriberRankingResult fourthWorstIMSIInRanking = rankingResults.get(3);
        assertThat(fourthWorstIMSIInRanking.getIMSI(), is(fourthWorstIMSI));
        assertThat(fourthWorstIMSIInRanking.getNoErrors(), is(noErrorsForFourthWorstImsiIn2G3GNetworks
                + noErrorsForFourthWorstImsiInLTENetworks));
        assertThat(fourthWorstIMSIInRanking.getNoSuccesses(), is("0"));

    }

    private void createTemporaryRawSucTable(final String tempRawSucTable) throws SQLException {
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
            final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();

            insertIntoAgg(SGEH_IMSI_RANK, worstIMSI, noErrorsForWorstImsiIn2G3GNetworks, sqlExecutor, dateTime);
            insertIntoAgg(LTE_IMSI_RANK, worstIMSI, noErrorsForWorstImsiInLTENetwork, sqlExecutor, dateTime);


            insertIntoAgg(SGEH_IMSI_RANK, secondWorstIMSI, noErrorsForSecondWorstImsi, sqlExecutor, dateTime);

            insertIntoAgg(SGEH_IMSI_RANK, thirdWorstIMSI, noErrorsForThirdWorstImsi, sqlExecutor, dateTime);

            insertIntoAgg(LTE_IMSI_RANK, fourthWorstIMSI, noErrorsForFourthWorstImsiInLTENetworks, sqlExecutor,
                    dateTime);
            insertIntoAgg(SGEH_IMSI_RANK, fourthWorstIMSI, noErrorsForFourthWorstImsiIn2G3GNetworks, sqlExecutor,
                    dateTime);

            insertIntoAgg(LTE_IMSI_RANK, IMSIZero, noErrorsForImsiZeroInLTENetworks, sqlExecutor,
                    dateTime);
            insertIntoAgg(SGEH_IMSI_RANK, IMSIZero, noErrorsForImsiZeroIn2G3GNetworks, sqlExecutor,
                    dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void insertIntoRaw(final String table, final String imsi, final SQLExecutor sqlExecutor,
            final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + imsi + "," + SOME_TAC + ",'" + dateTime + "')");
    }

    private void insertIntoAgg(final String table, final String imsi, final int numErrors,
            final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor
                .executeUpdate("insert into " + table + " values(" + imsi + "," + numErrors + ",'" + dateTime + "')");
    }

    private void createTemporaryIMSIRankingTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(IMSI unsigned bigint, NO_OF_ERRORS int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

      private void createTemporaryIMSIRankingSucTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(IMSI unsigned bigint, NO_OF_SUCCESSES int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

}
