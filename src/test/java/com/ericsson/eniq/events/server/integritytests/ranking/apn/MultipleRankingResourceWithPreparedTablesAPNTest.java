package com.ericsson.eniq.events.server.integritytests.ranking.apn;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleAPNRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedTablesAPNTest extends BaseDataIntegrityTest<MultipleAPNRankingResult> {

    private final GenericService multipleRankingService = new MultipleRankingService();

    private final static List<String> tempTables = new ArrayList<String>();

    private final int noSuccessesForWorstAPN = 3;

    private final int noSuccessesForSecondWorstAPN = 4;

    private final int noErrorsForWorstAPN = 7;

    private final int noErrorsForSecondWorstAPN = 6;

    private final String worstAPN = "the worst apn";

    private final String secondWorstAPN = "the second worst apn";

    static {
        tempTables.add("#EVENT_E_SGEH_APN_SUC_DAY");
        tempTables.add("#EVENT_E_SGEH_APN_ERR_DAY");
        tempTables.add("#EVENT_E_LTE_APN_SUC_DAY");
        tempTables.add("#EVENT_E_LTE_APN_ERR_DAY");

    }

    @Before
    public void onSetUp() throws Exception {
        attachDependencies(multipleRankingService);
        for (final String tempTable : tempTables) {
            createAndPopulateTemporaryTables(tempTable);
        }
        createAndPopulateTemporaryDIMTables();
    }

    @Test
    public void testGetRankingData_APN() throws Exception {

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");

        final String json = runQuery(multipleRankingService, map);
        System.out.println(json);
        final List<MultipleAPNRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleAPNRankingResult.class);
        assertThat(rankingResult.size(), is(2));
        final MultipleAPNRankingResult resultForworstAPN = rankingResult.get(0);
        assertThat(resultForworstAPN.getManufacturer(), is(worstAPN));
        assertThat(resultForworstAPN.getNoErrors(), is(Integer.toString(noErrorsForWorstAPN)));
        assertThat(resultForworstAPN.getNoSuccesses(), is(Integer.toString(noSuccessesForWorstAPN)));
        final MultipleAPNRankingResult resultForSecondWorstAPN = rankingResult.get(1);
        assertThat(resultForSecondWorstAPN.getManufacturer(), is(secondWorstAPN));
        assertThat(resultForSecondWorstAPN.getNoErrors(), is(Integer.toString(noErrorsForSecondWorstAPN)));
        assertThat(resultForSecondWorstAPN.getNoSuccesses(), is(Integer.toString(noSuccessesForSecondWorstAPN)));

    }

    private void createAndPopulateTemporaryDIMTables() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(APN);
        columns.add(LAST_SEEN);
        createTemporaryTable(TEMP_DIM_E_SGEH_APN, columns);
        createTemporaryTable(TEMP_DIM_E_LTE_APN, columns);

        final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();
        final Map<String, Object> rowForWorstAPN = new HashMap<String, Object>();
        rowForWorstAPN.put(APN, worstAPN);
        rowForWorstAPN.put(LAST_SEEN, dateTime);
        insertRow(TEMP_DIM_E_SGEH_APN, rowForWorstAPN);
        final Map<String, Object> rowForSecondWorstAPN = new HashMap<String, Object>();
        rowForSecondWorstAPN.put(APN, secondWorstAPN);
        rowForSecondWorstAPN.put(LAST_SEEN, dateTime);
        insertRow(TEMP_DIM_E_SGEH_APN, rowForSecondWorstAPN);

    }

    private void createAndPopulateTemporaryTables(final String tempTableName) throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(APN);
        columns.add(NO_OF_SUCCESSES);
        columns.add(NO_OF_ERRORS);
        columns.add(DATETIME_ID);
        createTemporaryTable(tempTableName, columns);

        final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();
        if (isSucTable(tempTableName)) {
            final Map<String, Object> valuesForWorstAPN = new HashMap<String, Object>();
            valuesForWorstAPN.put(APN, worstAPN);
            valuesForWorstAPN.put(NO_OF_SUCCESSES, noSuccessesForWorstAPN);
            valuesForWorstAPN.put(NO_OF_ERRORS, 0);
            valuesForWorstAPN.put(DATETIME_ID, dateTime);
            insertRow(tempTableName, valuesForWorstAPN);

            final Map<String, Object> valuesForSecondWorstAPN = new HashMap<String, Object>();
            valuesForSecondWorstAPN.put(APN, secondWorstAPN);
            valuesForSecondWorstAPN.put(NO_OF_SUCCESSES, noSuccessesForSecondWorstAPN);
            valuesForSecondWorstAPN.put(NO_OF_ERRORS, 0);
            valuesForSecondWorstAPN.put(DATETIME_ID, dateTime);
            insertRow(tempTableName, valuesForSecondWorstAPN);

        } else {

            final Map<String, Object> valuesForWorstAPN = new HashMap<String, Object>();
            valuesForWorstAPN.put(APN, worstAPN);
            valuesForWorstAPN.put(NO_OF_SUCCESSES, 0);
            valuesForWorstAPN.put(NO_OF_ERRORS, noErrorsForWorstAPN);
            valuesForWorstAPN.put(DATETIME_ID, dateTime);
            insertRow(tempTableName, valuesForWorstAPN);

            final Map<String, Object> valuesForSecondWorstAPN = new HashMap<String, Object>();
            valuesForSecondWorstAPN.put(APN, secondWorstAPN);
            valuesForSecondWorstAPN.put(NO_OF_SUCCESSES, 0);
            valuesForSecondWorstAPN.put(NO_OF_ERRORS, noErrorsForSecondWorstAPN);
            valuesForSecondWorstAPN.put(DATETIME_ID, dateTime);
            insertRow(tempTableName, valuesForSecondWorstAPN);

        }

    }

}
