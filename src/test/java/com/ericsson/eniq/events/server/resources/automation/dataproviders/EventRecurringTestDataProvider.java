package com.ericsson.eniq.events.server.resources.automation.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

public class EventRecurringTestDataProvider {

    private static final String GROUP_NAME = "group";

    private static final String TYPE_IMSI = "IMSI";

    private static final String CAUSE_CODE = "1";

    private static final String SUB_CAUSE_CODE = "1";

    private static final String EVENT_RESULT = "1";

    private static final String CAUSE_PROTTYPE = "1";

    private static final String EVENT_TYPE = "ATTACH";

    public static Object[] provideTestData() {

        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(GROUP_NAME_PARAM, GROUP_NAME)
                .add(TYPE_PARAM, TYPE_IMSI)
                .add(CAUSE_CODE_PARAM, CAUSE_CODE)
                .add(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE)
                .add(EVENT_TYPE_PARAM, EVENT_TYPE)
                .add(CAUSE_PROT_TYPE, CAUSE_PROTTYPE)
                .add(EVENT_RESULT_PARAM, EVENT_RESULT)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK, String.valueOf(MINUTES_IN_2_WEEKS))
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS)
                .build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}
