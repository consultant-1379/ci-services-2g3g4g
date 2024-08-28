package com.ericsson.eniq.events.server.integritytests.ranking.controller;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleControllerRankingResult;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedTablesENodeBTest extends
        BaseDataIntegrityTest<MultipleControllerRankingResult> {

    private final MultipleRankingService multipleRankingResource = new MultipleRankingService();

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingResource);
    }

    @Test
    public void testGetRankingData_ENODEB_TwoWeeks() throws Exception {

        final AggTablesPopulatorForMultipleControllerRanking tablesPopulator = new AggTablesPopulatorForMultipleControllerRanking();
        tablesPopulator.createAndPopulateAggTables(connection);

        final List<MultipleControllerRankingResult> rankingResult = getENodeBRankingData(TWO_WEEKS);
        assertThat(rankingResult.size(), is(2));

        final MultipleControllerRankingResult worstRankingENodeB = rankingResult.get(0);
        assertThat(worstRankingENodeB.getRATDesc(), is(LTE));
        assertThat(worstRankingENodeB.getController(), is(ERBS1));
        assertThat(worstRankingENodeB.getNoErrors(),
                is(AggTablesPopulatorForMultipleControllerRanking.noErrorsForWorstERBS_ERBS1));
        assertThat(worstRankingENodeB.getNoSuccesses(),
                is(AggTablesPopulatorForMultipleControllerRanking.noSuccessesInLteTable * 2));

        final MultipleControllerRankingResult secondWorstENodeB = rankingResult.get(1);
        assertThat(secondWorstENodeB.getRATDesc(), is(LTE));
        assertThat(secondWorstENodeB.getController(), is(ERBS2));
        assertThat(secondWorstENodeB.getNoErrors(),
                is(AggTablesPopulatorForMultipleControllerRanking.noErrorsForSecondWorstERBS_ERBS2));
        assertThat(secondWorstENodeB.getNoSuccesses(), is(0));

    }

    @Test
    public void testGetRankingDataWithDataTiering_ENodeB_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        new RawTablesPopulatorForMultipleControllerRanking().createAndPopulateRawTables(connection);
        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection);

        final List<MultipleControllerRankingResult> rankingResult = getENodeBRankingData(THIRTY_MINUTES);
        assertThat(rankingResult.size(), is(1));

        final MultipleControllerRankingResult worstRankingENodeB = rankingResult.get(0);
        assertThat(worstRankingENodeB.getRATDesc(), is(LTE));
        assertThat(worstRankingENodeB.getController(), is(ERBS1));
        assertThat(worstRankingENodeB.getNoErrors(), is(1));
        assertThat(worstRankingENodeB.getNoSuccesses(), is(2));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private List<MultipleControllerRankingResult> getENodeBRankingData(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_ENODEB);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingResource, map);
        System.out.println(json);
        final List<MultipleControllerRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleControllerRankingResult.class);
        return rankingResult;
    }
}
