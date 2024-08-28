package com.ericsson.eniq.events.server.integritytests.eventanalysis.manufacturer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.ManufacturerEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.ACTIVATE;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.EVENT_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.KEY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.KEY_TYPE_SUM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_ATTACH;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.L_SERVICE_REQUEST;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAN_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.SERVICE_REQUEST;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_MAN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ACTIVATE_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ATTACH_IN_4G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.SERVICE_REQUEST_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.common.EventIDConstants.SERVICE_REQUEST_IN_4G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_LTE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SONY_ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SONY_ERICSSON_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_LTE_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventAnalysisSummaryWithPreparedRawTablesManufacturerTest extends
        BaseDataIntegrityTest<ManufacturerEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private final static List<String> tempTables = new ArrayList<String>();

    static {
        tempTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

    }

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);
        for (final String tempTable : tempTables) {
            final Collection<String> columnsInRawTables = new ArrayList<String>();
            columnsInRawTables.add(EVENT_ID);
            columnsInRawTables.add(TAC);
            columnsInRawTables.add(IMSI);
            columnsInRawTables.add(DATETIME_ID);
            columnsInRawTables.add(RAT);
            createTemporaryTable(tempTable, columnsInRawTables);
        }
    }

    @Test
    public void testGetSummaryDataWhenOnlyOneSuccessEventExists() throws Exception {
        populateTemporaryTablesWithJustOneSuccessEvent();
        final String json = getData();
        validateOneEventReturned(json);
    }

    private void validateOneEventReturned(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(firstResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(firstResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(firstResult.getErrorCount(), is((0)));
        assertThat(firstResult.getSuccessCount(), is((1)));
        assertThat(firstResult.getOccurrences(), is((1)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((0)));

    }

    private void populateTemporaryTablesWithJustOneSuccessEvent() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -45);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, RAT_FOR_LTE, dateTime);

    }

    @Test
    public void testGetSummaryData_Manufacturer_60Minutes() throws Exception {
        populateTemporaryTablesWithFullDataSet();
        final String json = getData();
        validateResultForFullDataSet(json);
    }

    @Test
    public void testGetSummaryData_Manufacturer_TACExclusion() throws Exception {
        populateTemporaryTablesWithFullDataSet();
        populateGroupTableWithExTACGroup();
        final String json = getData();
        validateResultIsEmpty(json);
    }

    private void validateResultIsEmpty(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(0));
    }

    private void populateGroupTableWithExTACGroup() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private String getData() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, "60");
        map.putSingle(MAN_PARAM, SONY_ERICSSON);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void validateResultForFullDataSet(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((2)));
        assertThat(firstResult.getSuccessCount(), is((2)));
        assertThat(firstResult.getOccurrences(), is((4)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((2)));
        assertThat(thirdResult.getOccurrences(), is((3)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((1)));
        assertThat(secondResult.getSuccessCount(), is((1)));
        assertThat(secondResult.getOccurrences(), is((2)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult fourthResult = summaryResult.get(3);
        assertThat(fourthResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(fourthResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(fourthResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(fourthResult.getErrorCount(), is((0)));
        assertThat(fourthResult.getSuccessCount(), is((1)));
        assertThat(fourthResult.getOccurrences(), is((1)));
        assertThat(fourthResult.getSuccessRatio(), is(fourthResult.getExpectedSuccessRatio()));
        assertThat(fourthResult.getErrorSubscriberCount(), is((0)));
    }

    private void populateTemporaryTablesWithFullDataSet() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -45);

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345671, RAT_FOR_GSM,
                dateTime);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345672, RAT_FOR_GSM,
                dateTime);

        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345673, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345674, RAT_FOR_GSM,
                dateTime);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345674, RAT_FOR_GSM,
                dateTime);

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345675, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, 12345676, RAT_FOR_LTE, dateTime);

        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345678, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, 12345676, RAT_FOR_LTE, dateTime);

    }

    private void insertRow(final String table, final int eventId, final int tac, final int imsi, final int rat,
            final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(RAT, rat);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

}
