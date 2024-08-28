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

public class MultipleRankingResourceWithPreparedTablesBSCTest extends
        BaseDataIntegrityTest<MultipleControllerRankingResult> {

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingService);
    }

    @Test
    public void testRankingDataWithDataTiered_BSC_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        new RawTablesPopulatorForMultipleControllerRanking().createAndPopulateRawTables(connection);
        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection);

        final List<MultipleControllerRankingResult> rankingResult = getRNCRanking(THIRTY_MINUTES);
        assertThat(rankingResult.size(), is(1));
        final MultipleControllerRankingResult worstRankingBSC = rankingResult.get(0);
        assertThat(worstRankingBSC.getRATDesc(), is(GSM));
        assertThat(worstRankingBSC.getController(), is(BSC1));
        assertThat(worstRankingBSC.getNoErrors(), is(1));
        assertThat(worstRankingBSC.getNoSuccesses(), is(3));

        jndiProperties.setUpJNDIPropertiesForTest();

    }

    @Test
    public void testGetRankingData_BSC_TwoWeeks() throws Exception {

        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection);

        final List<MultipleControllerRankingResult> rankingResult = getRNCRanking(TWO_WEEKS);
        assertThat(rankingResult.size(), is(2));

        final MultipleControllerRankingResult worstRankingBSC = rankingResult.get(0);
        assertThat(worstRankingBSC.getRATDesc(), is(GSM));
        assertThat(worstRankingBSC.getController(), is(BSC1));
        assertThat(worstRankingBSC.getNoErrors(),
                is(AggTablesPopulatorForMultipleControllerRanking.noErrorsForWorstBSC_BSC1));

        final MultipleControllerRankingResult secondWorstBSC = rankingResult.get(1);
        assertThat(secondWorstBSC.getRATDesc(), is(GSM));
        assertThat(secondWorstBSC.getController(), is(BSC2));
        assertThat(secondWorstBSC.getNoErrors(),
                is(AggTablesPopulatorForMultipleControllerRanking.noErrorsForSecondWorstBSC_BSC2 * 2));

    }

    private List<MultipleControllerRankingResult> getRNCRanking(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingService, map);
        System.out.println(json);
        final List<MultipleControllerRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleControllerRankingResult.class);
        return rankingResult;
    }

}
