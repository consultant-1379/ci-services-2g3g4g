package com.ericsson.eniq.events.server.integritytests.ranking.accessarea;

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
import com.ericsson.eniq.events.server.test.queryresults.MultipleAccessAreaRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedTablesAccessAreaTest extends
        BaseDataIntegrityTest<MultipleAccessAreaRankingResult> {

    private final int SGEH_LATENCY = 0;

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingService);
    }

    @Test
    public void testGetRankingData_AccessArea_5Minutes_IncludesDataFromSGEHANDLTETables() throws Exception {
        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection,
                DateTimeUtilities.getDateTimeMinus2Minutes());
        final List<MultipleAccessAreaRankingResult> rankingResult = getAccessAreaRanking(FIVE_MINUTES);

        assertThat(rankingResult.size(), is(4));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(GSM));
        assertThat(worstRankingCell.getController(), is(BSC1));
        assertThat(worstRankingCell.getCell(), is(GSMCELL1));
        assertThat(worstRankingCell.getNoErrors(), is(4));
        assertThat(worstRankingCell.getNoSuccesses(), is("1"));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS1));
        assertThat(secondWorstCell.getCell(), is(LTECELL1));
        assertThat(secondWorstCell.getNoErrors(), is(3));
        assertThat(secondWorstCell.getNoSuccesses(), is("1"));

        final MultipleAccessAreaRankingResult thirdWorstCell = rankingResult.get(2);
        assertThat(thirdWorstCell.getRATDesc(), is(GSM));
        assertThat(thirdWorstCell.getController(), is(BSC1));
        assertThat(thirdWorstCell.getCell(), is(GSMCELL2));
        assertThat(thirdWorstCell.getNoErrors(), is(2));
        assertThat(thirdWorstCell.getNoSuccesses(), is("1"));

        final MultipleAccessAreaRankingResult fourthWorstCell = rankingResult.get(3);
        assertThat(fourthWorstCell.getRATDesc(), is(LTE));
        assertThat(fourthWorstCell.getController(), is(ERBS2));
        assertThat(fourthWorstCell.getCell(), is(LTECELL2));
        assertThat(fourthWorstCell.getNoErrors(), is(1));
        assertThat(fourthWorstCell.getNoSuccesses(), is("1"));
    }

    @Test
    public void testGetRankingData_AccessArea_IncludesDataFromSGEHAndLTETables() throws Exception {

        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection);

        final List<MultipleAccessAreaRankingResult> rankingResult = getAccessAreaRanking(String
                .valueOf(MINUTES_IN_2_WEEKS));
        assertThat(rankingResult.size(), is(4));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(GSM));
        assertThat(worstRankingCell.getController(), is(BSC1));
        assertThat(worstRankingCell.getCell(), is(GSMCELL1));
        assertThat(worstRankingCell.getNoErrors(),
                is(AggregationTablesPopulatorForMultipleCellRanking.noErrorsForWorstCell_GSMCELL1));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS1));
        assertThat(secondWorstCell.getNoErrors(),
                is(AggregationTablesPopulatorForMultipleCellRanking.noErrorsForSecondWorstCell_LTECELL1));

        final MultipleAccessAreaRankingResult thirdWorstCell = rankingResult.get(2);
        assertThat(thirdWorstCell.getRATDesc(), is(GSM));
        assertThat(thirdWorstCell.getController(), is(BSC1));
        assertThat(thirdWorstCell.getCell(), is(GSMCELL2));
        assertThat(thirdWorstCell.getNoErrors(), is(11));

        final MultipleAccessAreaRankingResult fourthWorstCell = rankingResult.get(3);
        assertThat(fourthWorstCell.getRATDesc(), is(LTE));
        assertThat(fourthWorstCell.getController(), is(ERBS1));
        assertThat(fourthWorstCell.getCell(), is(LTECELL2));
        assertThat(fourthWorstCell.getNoErrors(), is(4));
    }

    @Test
    public void getAccessAreaRankingWithDataTieringOn30Min() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();

        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection,
                DateTimeUtilities.getDateTimeMinus25Minutes());
        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection);

        final List<MultipleAccessAreaRankingResult> rankingResult = getAccessAreaRanking(THIRTY_MINUTES);
        assertThat(rankingResult.size(), is(4));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(GSM));
        assertThat(worstRankingCell.getController(), is(BSC1));
        assertThat(worstRankingCell.getCell(), is(GSMCELL1));
        assertThat(worstRankingCell.getNoErrors(), is(4));
        assertThat(worstRankingCell.getNoSuccesses(), is("3"));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS1));
        assertThat(secondWorstCell.getCell(), is(LTECELL1));
        assertThat(secondWorstCell.getNoErrors(), is(3));
        assertThat(secondWorstCell.getNoSuccesses(), is("17"));

        final MultipleAccessAreaRankingResult thirdWorstCell = rankingResult.get(2);
        assertThat(thirdWorstCell.getRATDesc(), is(GSM));
        assertThat(thirdWorstCell.getController(), is(BSC1));
        assertThat(thirdWorstCell.getCell(), is(GSMCELL2));
        assertThat(thirdWorstCell.getNoErrors(), is(2));
        assertThat(thirdWorstCell.getNoSuccesses(), is("0"));

        final MultipleAccessAreaRankingResult fourthWorstCell = rankingResult.get(3);
        assertThat(fourthWorstCell.getRATDesc(), is(LTE));
        assertThat(fourthWorstCell.getController(), is(ERBS2));
        assertThat(fourthWorstCell.getCell(), is(LTECELL2));
        assertThat(fourthWorstCell.getNoErrors(), is(1));
        assertThat(fourthWorstCell.getNoSuccesses(), is("0"));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private List<MultipleAccessAreaRankingResult> getAccessAreaRanking(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingService, map);
        System.out.println(json);
        final List<MultipleAccessAreaRankingResult> rankingResult = getTranslator().translateResult(json,
                MultipleAccessAreaRankingResult.class);
        return rankingResult;
    }
}
