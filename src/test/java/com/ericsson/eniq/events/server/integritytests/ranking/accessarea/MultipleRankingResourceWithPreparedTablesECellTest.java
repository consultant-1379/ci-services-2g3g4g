/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
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

/**
 * @author edivkir
 * @since 2011
 *
 */
public class MultipleRankingResourceWithPreparedTablesECellTest extends
        BaseDataIntegrityTest<MultipleAccessAreaRankingResult> {

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingService);
    }

    @Test
    public void testGetRankingData_ECELL_5Minutes() throws Exception {

        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection,
                DateTimeUtilities.getDateTimeMinus2Minutes());

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(FIVE_MINUTES);
        assertThat(rankingResult.size(), is(2));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(LTE));
        assertThat(worstRankingCell.getController(), is(ERBS1));
        assertThat(worstRankingCell.getCell(), is(LTECELL1));
        assertThat(worstRankingCell.getNoErrors(), is(3));
        assertThat(worstRankingCell.getNoSuccesses(), is("1"));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS2));
        assertThat(secondWorstCell.getCell(), is(LTECELL2));
        assertThat(secondWorstCell.getNoErrors(), is(1));
        assertThat(secondWorstCell.getNoSuccesses(), is("1"));
    }

    @Test
    public void testGetRankingDataWithDataTiering_ECELL_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();

        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection,
                DateTimeUtilities.getDateTimeMinus25Minutes());
        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(THIRTY_MINUTES);
        assertThat(rankingResult.size(), is(2));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(LTE));
        assertThat(worstRankingCell.getController(), is(ERBS1));
        assertThat(worstRankingCell.getCell(), is(LTECELL1));
        assertThat(worstRankingCell.getNoErrors(), is(3));
        assertThat(worstRankingCell.getNoSuccesses(), is("17"));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS2));
        assertThat(secondWorstCell.getCell(), is(LTECELL2));
        assertThat(secondWorstCell.getNoErrors(), is(1));
        assertThat(secondWorstCell.getNoSuccesses(), is("0"));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetRankingData_ECELL_TwoWeeks() throws Exception {

        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(TWO_WEEKS);
        assertThat(rankingResult.size(), is(2));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(LTE));
        assertThat(worstRankingCell.getController(), is(ERBS1));
        assertThat(worstRankingCell.getCell(), is(LTECELL1));
        assertThat(worstRankingCell.getNoErrors(),
                is(AggregationTablesPopulatorForMultipleCellRanking.noErrorsForSecondWorstCell_LTECELL1));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS1));
        assertThat(secondWorstCell.getCell(), is(LTECELL2));
        assertThat(secondWorstCell.getNoErrors(), is(4));

    }

    private List<MultipleAccessAreaRankingResult> getECellRanking(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_ECELL);
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
