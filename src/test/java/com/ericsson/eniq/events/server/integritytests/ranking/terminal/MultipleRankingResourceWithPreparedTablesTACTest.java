package com.ericsson.eniq.events.server.integritytests.ranking.terminal;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleTACRankingResult;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedTablesTACTest extends BaseDataIntegrityTest<MultipleTACRankingResult> {

    private static final String SONY_ERICSSON = "Sony Ericsson";

    private static final String MOTOROLA = "Motorola";

    private static final String NORTEL_TAC = "100300";

    private final static List<String> tempTables = new ArrayList<String>();

    private final int noSuccessesSonyEricsson = 3;

    private final int noSuccessesMotorola = 4;

    private final int noErrorsSonyEricsson = 4;

    private final int noErrorsMotorola = 6;

    private MultipleRankingService multipleRankingResource;

    static {
        tempTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY);
        tempTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY);
        tempTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY);
        tempTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY);
    }

    @Before
    public void onSetUp() {
        multipleRankingResource = new MultipleRankingService();
        attachDependencies(multipleRankingResource);
    }

    @Test
    public void testGetRankingData_TAC() throws Exception {

        for (final String tempTable : tempTables) {
            createAndPopulateTemporaryTables(tempTable);
        }

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingResource, map);
        System.out.println(json);
        final ResultTranslator<MultipleTACRankingResult> rt = getTranslator();
        final List<MultipleTACRankingResult> rankingResult = rt.translateResult(json, MultipleTACRankingResult.class);
        assertThat(rankingResult.size(), is(2));
        final MultipleTACRankingResult worstTac = rankingResult.get(0);
        assertThat(worstTac.getManufacturer(), is(MOTOROLA));
        assertThat(worstTac.getNoErrors(), is(Integer.toString(noErrorsMotorola * 2)));
        assertThat(worstTac.getNoSuccesses(), is(Integer.toString(noSuccessesMotorola * 2)));
        final MultipleTACRankingResult nextWorstTac = rankingResult.get(1);
        assertThat(nextWorstTac.getManufacturer(), is(SONY_ERICSSON));
        assertThat(nextWorstTac.getNoErrors(), is(Integer.toString(noErrorsSonyEricsson * 2)));
        assertThat(nextWorstTac.getNoSuccesses(), is(Integer.toString(noSuccessesSonyEricsson * 2)));

    }

    private void createAndPopulateTemporaryTables(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(MANUFACTURER varchar(128), TAC int, NO_OF_SUCCESSES int, "
                    + "NO_OF_ERRORS int, DATETIME_ID timestamp)");
            final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();
            if (isSucTable(tempTableName)) {
                sqlExecutor.executeUpdate("insert into " + tempTableName + " values('" + SONY_ERICSSON + "',"
                        + NORTEL_TAC + "," + noSuccessesSonyEricsson + ",0,'" + dateTime + "')");
                sqlExecutor.executeUpdate("insert into " + tempTableName + " values('" + MOTOROLA + "'," + "100500"
                        + "," + noSuccessesMotorola + ",0,'" + dateTime + "')");
            } else {
                sqlExecutor.executeUpdate("insert into " + tempTableName + " values('" + SONY_ERICSSON + "',"
                        + NORTEL_TAC + ",0," + noErrorsSonyEricsson + ",'" + dateTime + "')");
                sqlExecutor.executeUpdate("insert into " + tempTableName + " values('" + MOTOROLA + "'," + "100500"
                        + ",0," + noErrorsMotorola + ",'" + dateTime + "')");
            }
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

}
