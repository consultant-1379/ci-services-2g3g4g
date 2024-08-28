package com.ericsson.eniq.events.server.resources.automation.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

public class EventVolumeTestDataProvider {

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "11052010";

    private static final String DATE_TO = "12052010";

    public static Object[] provideTestDataForNoType() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, GRID_PARAM, CHART_PARAM)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForTimeAndDatesFromAndTo() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TIME_FROM_QUERY_PARAM, TIME_FROM)
                .add(DATE_FROM_QUERY_PARAM, DATE_FROM).add(TIME_TO_QUERY_PARAM, TIME_TO)
                .add(DATE_TO_QUERY_PARAM, DATE_TO).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForSGSN() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_SGSN).add(NODE_PARAM, SAMPLE_SGSN)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForSGSNGroup() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_SGSN).add(GROUP_NAME_PARAM, SAMPLE_SGSN_GROUP)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForBSC() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_BSC).add(NODE_PARAM, TEST_VALUE_BSC)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForBSCGroup() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_BSC).add(GROUP_NAME_PARAM, SAMPLE_BSC_GROUP)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForCell() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_CELL)
                .add(NODE_PARAM, TEST_VALUE_ACCESS_AREA_NODE).add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForCellGroup() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_CELL).add(GROUP_NAME_PARAM, SAMPLE_CELL_GROUP)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForAPN() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_SGSN).add(NODE_PARAM, SAMPLE_SGSN)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForAPNGroup() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TYPE_PARAM, TYPE_APN).add(GROUP_NAME_PARAM, SAMPLE_APN_GROUP)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

}
