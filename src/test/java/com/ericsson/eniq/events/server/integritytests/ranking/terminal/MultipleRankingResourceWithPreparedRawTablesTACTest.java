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
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedRawTablesTACTest extends
        BaseDataIntegrityTest<MultipleTACRankingResult> {

    private static final String SONY_ERICSSON = "Sony Ericsson";

    private static final String MOTOROLA = "Motorola";

    private static final String SIEMENS = "Siemens";

    private static final int SONY_ERICSSON_TAC = 100300;

    private static final int MOTOROLA_TAC = 100500;

    private static final int SIEMENS_TAC = 100200;

    private static final int SONY_ERICSSON_NO_OF_SUCCESS = 2;

    private static final int ZERO = 0;

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static List<String> tempAggTables = new ArrayList<String>();

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    static {
        tempRawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        tempAggTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN);
        tempAggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN);
    }

    @Before
    public void onSetUp() throws Exception {
        attachDependencies(multipleRankingService);
    }

    @Test
    public void testGetRankingDataWithDataTiering_TAC_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        setUpTempTableAndData(true, DateTimeUtilities.getDateTimeMinus25Minutes());
        final String json = getData(multipleRankingService, getURL(THIRTY_MINUTES));
        System.out.println(json);

        final List<MultipleTACRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleTACRankingResult.class);
        assertThat(rankingResult.size(), is(3));
        final MultipleTACRankingResult worstTac = rankingResult.get(0);
        assertThat(worstTac.getManufacturer(), is(MOTOROLA));
        assertThat(worstTac.getNoErrors(), is("4"));
        assertThat(worstTac.getNoSuccesses(), is("0"));

        final MultipleTACRankingResult nextWorstTac = rankingResult.get(1);
        assertThat(nextWorstTac.getManufacturer(), is(SONY_ERICSSON));
        assertThat(nextWorstTac.getNoErrors(), is(Integer.toString(2)));
        assertThat(nextWorstTac.getNoSuccesses(), is(Integer.toString(SONY_ERICSSON_NO_OF_SUCCESS)));

        final MultipleTACRankingResult thirdWorstTac = rankingResult.get(2);
        assertThat(thirdWorstTac.getManufacturer(), is(SIEMENS));
        assertThat(thirdWorstTac.getNoErrors(), is("1"));
        assertThat(thirdWorstTac.getNoSuccesses(), is("0"));

        jndiProperties.setUpJNDIPropertiesForTest();

    }

    private void setUpTempTableAndData(final boolean dataTiering, final String dateTime) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            for (final String tempTable : tempRawTables) {
                sqlExecutor.executeUpdate("create local temporary table " + tempTable
                        + "(MANUFACTURER varchar(128), TAC int, DATETIME_ID timestamp)");
            }

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, MOTOROLA, MOTOROLA_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, MOTOROLA, MOTOROLA_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, MOTOROLA, MOTOROLA_TAC, sqlExecutor, dateTime, false, ZERO);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, MOTOROLA, MOTOROLA_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, MOTOROLA, MOTOROLA_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, MOTOROLA, MOTOROLA_TAC, sqlExecutor, dateTime, false, ZERO);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SONY_ERICSSON, SONY_ERICSSON_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SONY_ERICSSON, SONY_ERICSSON_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SONY_ERICSSON, SONY_ERICSSON_TAC, sqlExecutor, dateTime, false, ZERO);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, SIEMENS, SIEMENS_TAC, sqlExecutor, dateTime, false, ZERO);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, SIEMENS, SIEMENS_TAC, sqlExecutor, dateTime, false, ZERO);

            if (dataTiering) {
                for (final String tempTable : tempAggTables) {
                    sqlExecutor.executeUpdate("create local temporary table " + tempTable
                            + "(MANUFACTURER varchar(128), TAC int, NO_OF_SUCCESSES int, "
                            + "NO_OF_ERRORS int, DATETIME_ID timestamp)");
                }
                insertRow(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, SONY_ERICSSON, SONY_ERICSSON_TAC, sqlExecutor,
                        dateTime, true, SONY_ERICSSON_NO_OF_SUCCESS);
            }

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private void insertRow(final String table, final String manufacturer, final int tac, final SQLExecutor sqlExecutor,
            final String dateTime, final boolean isAggData, final int numOfSuccess) throws SQLException {

        if (isAggData) {
            sqlExecutor.executeUpdate("insert into " + table + " values('" + manufacturer + "'," + tac + ","
                    + numOfSuccess + ",0,'" + dateTime + "')");
        } else {
            sqlExecutor.executeUpdate("insert into " + table + " values('" + manufacturer + "'," + tac + ",'"
                    + dateTime + "')");
        }

    }

    private MultivaluedMap<String, String> getURL(final String time) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        return map;
    }

}
