/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.kpi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.kpi.KPIQueryfactory;
import com.ericsson.eniq.events.server.kpi.KpiFactory;
import com.ericsson.eniq.events.server.kpi.KpiUtilities;
import com.ericsson.eniq.events.server.resources.KPIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

/**
 * @author ejoegaf
 * @since 2011
 */
public abstract class BaseKPIAnalysisTest extends TestsWithTemporaryTablesBaseTestCase<KPIAnalysisResult> {

    protected final KPIResource kpiResource = new KPIResource();

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static Collection<String> columnsInRawTables = new ArrayList<String>();

    private final static List<String> tempAggTables = new ArrayList<String>();

    private final static Collection<String> columnsInAggTables = new ArrayList<String>();

    private final static Collection<String> columnsInEvntsrcGroupTable = new ArrayList<String>();

    private final static Collection<String> columnsInTacGroupTable = new ArrayList<String>();

    private final static double ZERO = 0.00;

    private final static double FIFTY = 50.00;

    private Connection anotherConnection;

    static {

        tempRawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);

        columnsInRawTables.add(EVENT_ID);
        columnsInRawTables.add(IMSI);
        columnsInRawTables.add(EVENT_SOURCE_NAME);
        columnsInRawTables.add(PAGING_ATTEMPTS);
        columnsInRawTables.add(DEACTIVATION_TRIGGER);
        columnsInRawTables.add(EVENT_SUBTYPE_ID);
        columnsInRawTables.add(TAC);
        columnsInRawTables.add(DATETIME_ID);

        tempAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_15MIN);
        tempAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_15MIN);
        tempAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_15MIN);
        tempAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_15MIN);
        tempAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_1MIN);
        tempAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_1MIN);

        columnsInAggTables.add(EVENT_ID);
        columnsInAggTables.add(EVENT_SOURCE_NAME);
        columnsInAggTables.add(NO_OF_PAGING_ATTEMPTS);
        columnsInAggTables.add(NO_OF_ERRORS);
        columnsInAggTables.add(NO_OF_SUCCESSES);
        columnsInAggTables.add(EVENT_SUBTYPE_ID);
        columnsInAggTables.add(DATETIME_ID);
        columnsInAggTables.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsInAggTables.add(NO_OF_TOTAL_ERR_SUBSCRIBERS);

        columnsInEvntsrcGroupTable.add(GROUP_NAME);
        columnsInEvntsrcGroupTable.add(EVENT_SOURCE_NAME);

        columnsInTacGroupTable.add(GROUP_NAME);
        columnsInTacGroupTable.add(TAC);

    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();

        final KPIQueryfactory queryFactory = new KPIQueryfactory();
        final KpiUtilities kpiUtilities = new KpiUtilities();
        kpiUtilities.setTemplateUtils(templateUtils);
        kpiUtilities.applicationStartup();
        queryFactory.setKpiUtilities(kpiUtilities);
        final KpiFactory kpiFactory = new KpiFactory();
        kpiFactory.setKpiUtilities(kpiUtilities);
        queryFactory.setKpiFactory(kpiFactory);
        kpiResource.setLteQueryBuilder(queryFactory);
        attachDependencies(kpiResource);
        createTemporaryTables();
        addSGSNtoGroup();
        addConnectionToInterceptedDbConnectionManager();
    }

    /**
     * This method will create another db connection and add it to the interceptedDbConnectionManager.
     * This is necessary if the code will require a second db request because DataServiceBean will close the
     * connection after each query, e.g. the code will verify if a TAC is in the EXCLUSIVE_TAC group before
     * sending an event analysis.
     *
     * @throws Exception
     */
    protected void addConnectionToInterceptedDbConnectionManager() throws Exception {
        anotherConnection = getDWHDataSourceConnection();
        createTemporaryTablesOnSpecificConnection(anotherConnection);
        createGroupTablesOnSpecificConnection(anotherConnection);
        interceptedDbConnectionManager.addConnection(anotherConnection);
    }

    private void createTemporaryTables() throws Exception {
        for (final String tempRawTable : tempRawTables) {
            createTemporaryTable(tempRawTable, columnsInRawTables);
        }
        for (final String tempAggTable : tempAggTables) {
            createTemporaryTable(tempAggTable, columnsInAggTables);
        }

        createGroupTables();

    }

    private void addSGSNtoGroup() throws Exception {

        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_MME_GROUP);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);

        insertRow(TEMP_GROUP_TYPE_E_EVNTSRC, values);
    }

    protected void addTACtoTACGroup(final int tac, final String tacGroupName) throws Exception {

        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, tacGroupName);
        values.put(TAC, tac);

        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createGroupTables() throws Exception {
        createTemporaryTable(TEMP_GROUP_TYPE_E_EVNTSRC, columnsInEvntsrcGroupTable);
    }

    private void createGroupTablesOnSpecificConnection(final Connection conn) throws Exception {
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_EVNTSRC, columnsInEvntsrcGroupTable);
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_TAC, columnsInTacGroupTable);

    }

    private void createTemporaryTablesOnSpecificConnection(final Connection conn) throws Exception {
        for (final String tempRawTable : tempRawTables) {
            createTemporaryTableOnSpecificConnection(conn, tempRawTable, columnsInRawTables);
        }
        for (final String tempAggTable : tempAggTables) {
            createTemporaryTableOnSpecificConnection(conn, tempAggTable, columnsInAggTables);
        }
    }

    protected String populateRawTemporaryTablesWithJustOneSuccessAndOneFailEvent(final int testBaseMinsAgo)
            throws SQLException {
        final Map<Integer, String> dateTimeMap = DateTimeUtilities.getDateTimeMap(testBaseMinsAgo, 1);

        final String dateTime = dateTimeMap.get(testBaseMinsAgo);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 1, 1, 0,
                SAMPLE_TAC, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 1, 1, 0,
                SAMPLE_TAC, dateTime);

        return dateTime;
    }

    protected String populateRawTemporaryTablesWithManyEvents(final int testBaseMinsAgo) throws SQLException {
        final int noTimeSlots = 1;
        final Map<Integer, String> dateTimeMap = DateTimeUtilities.getDateTimeMap(testBaseMinsAgo, noTimeSlots);

        final String dateTime = dateTimeMap.get(testBaseMinsAgo);
        for (int i = 0; i < 25; i++) {
            insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 1, 1, 0,
                    SAMPLE_TAC, dateTime);
        }
        for (int i = 0; i < 12; i++) {
            insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 0, 1, 0,
                    SAMPLE_TAC, dateTime);
        }
        for (int i = 0; i < 15; i++) {
            insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 1, 1, 0,
                    SAMPLE_TAC, dateTime);
        }
        for (int i = 0; i < 11; i++) {
            insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 0, 1, 0,
                    SAMPLE_TAC, dateTime);
        }
        for (int i = 0; i < 11; i++) {
            insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI_2, SAMPLE_MME, 0, 1, 0,
                    SAMPLE_TAC, dateTime);
        }
        for (int i = 0; i < 11; i++) {
            insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI_3, SAMPLE_MME, 0, 1, 0,
                    SAMPLE_TAC, dateTime);
        }

        return dateTime;

    }

    protected void populateAggregatedTemporaryTablesWithManyEvents(final String dateTime) throws SQLException {

        insertRowIntoAggTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_15MIN, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 10, 0, 25,
                0, dateTime, 0, 0);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_15MIN, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 10, 15, 0,
                0, dateTime, 0, 1);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_15MIN, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 0, 0, 12,
                0, dateTime, 0, 0);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_15MIN, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 0, 33, 0,
                0, dateTime, 0, 3);
    }

    protected void populateAggregatedTemporaryTablesWithJustOneSuccessAndOneFailEvent(final String dateTime)
            throws SQLException {

        insertRowIntoAggTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_1MIN, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 1, 0, 1, 0,
                dateTime, 0, 0);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_1MIN, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 1, 1, 0, 0,
                dateTime, 0, 1);

    }

    protected void validateRawResultForOneSuccessAndOneFailEvent(final String json, final String dateTime)
            throws Exception {
        final List<KPIAnalysisResult> kpiResults = getTranslator().translateResult(json, KPIAnalysisResult.class);
        assertThat(kpiResults.size(), is(5));
        final KPIAnalysisResult result = getKPIResultByTime(kpiResults, dateTime);
        assertNotNull(result);
        validateSingleResultForOneSuccessAndOneFailEvent(result);
    }

    protected void validateRawResultIsAllZerosForForManyEvents(final String json, final String dateTime)
            throws Exception {
        final List<KPIAnalysisResult> kpiResults = getTranslator().translateResult(json, KPIAnalysisResult.class);
        assertThat(kpiResults.size(), is(5));
        final KPIAnalysisResult result = getKPIResultByTime(kpiResults, dateTime);
        assertNotNull(result);
        validateSingleResultIsAllZeros(result);
    }

    protected void validateRawResultForForManyEvents(final String json, final String dateTime) throws Exception {
        final List<KPIAnalysisResult> kpiResults = getTranslator().translateResult(json, KPIAnalysisResult.class);
        assertThat(kpiResults.size(), is(5));
        final KPIAnalysisResult result = getKPIResultByTime(kpiResults, dateTime);
        assertNotNull(result);
        validateSingleResultForManyEvents(result);
    }

    private KPIAnalysisResult getKPIResultByTime(final List<KPIAnalysisResult> kpiResults, final String dateTime) {
        for (final KPIAnalysisResult kpiResult : kpiResults) {
            if (StringUtils.equals(kpiResult.getDateTimeStr(), dateTime + ".0")) {
                return kpiResult;
            }
        }
        return null;
    }

    protected void validateAggResultForOneSuccessAndOneFailEvent(final String json, final String dateTime,
            final int expectedNumberOfResults) throws Exception {
        final List<KPIAnalysisResult> kpiResults = getTranslator().translateResult(json, KPIAnalysisResult.class);
        assertThat(kpiResults.size(), is(expectedNumberOfResults));
        final KPIAnalysisResult result = getKPIResultByTime(kpiResults, dateTime);
        assertNotNull(result);
        validateSingleResultForOneSuccessAndOneFailEvent(result);
    }

    protected void validateAggResultForManyEvents(final String json, final String dateTime,
            final int expectedNumberOfResults) throws Exception {
        final List<KPIAnalysisResult> kpiResults = getTranslator().translateResult(json, KPIAnalysisResult.class);
        assertThat(kpiResults.size(), is(expectedNumberOfResults));
        final KPIAnalysisResult result = getKPIResultByTime(kpiResults, dateTime);
        assertNotNull(result);
        validateSingleResultForManyEvents(result);
    }

    private void validateSingleResultForOneSuccessAndOneFailEvent(final KPIAnalysisResult kpiResult) throws Exception {

        assertThat(kpiResult.getAttachSuccessRate(), is(ZERO));
        assertThat(kpiResult.getPdpContextActSR(), is(ZERO));
        assertThat(kpiResult.getRaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getInterSGSN_MMERAUpdateSR(), is(ZERO));
        assertThat(kpiResult.getPdpContextCutoffRatio(), is(ZERO));
        assertThat(kpiResult.getDetachSuccessRate(), is(ZERO));
        assertThat(kpiResult.getServiceRequestFailureRatio(), is(ZERO));
        assertThat(kpiResult.getPagingFailureRatio(), is(ZERO));
        assertThat(kpiResult.getPagingattemptsperSubs(), is(ZERO));
        assertThat(kpiResult.getImpactedSubscribers(), is("0"));
        assertThat(kpiResult.getAttachSuccessRateLTE(), is(ZERO));
        assertThat(kpiResult.getPdnConnectionSuccessRate(), is(ZERO));
        assertThat(kpiResult.getImpactedSubscribersLTE(), is("1"));
        assertThat(kpiResult.getBearerActivationSuccessRate(), is(ZERO));
        assertThat(kpiResult.getUeInitiatedServiceRequestFailureRatioLTE(), is(ZERO));
        assertThat(kpiResult.getPagingFailureRatioLTE(), is(FIFTY));
        assertThat(kpiResult.getTrackingAreaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getInterMMETrackingAreaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getX2basedhandover(), is(ZERO));
        assertThat(kpiResult.getX2basedHOwithoutSGWrelocation(), is(ZERO));
        assertThat(kpiResult.getX2basedHOwithSGWrelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedhandover(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandwithMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandwithoutMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandMMErelocation(), is(ZERO));

    }

    private void validateSingleResultIsAllZeros(final KPIAnalysisResult kpiResult) throws Exception {

        assertThat(kpiResult.getAttachSuccessRate(), is(ZERO));
        assertThat(kpiResult.getPdpContextActSR(), is(ZERO));
        assertThat(kpiResult.getRaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getInterSGSN_MMERAUpdateSR(), is(ZERO));
        assertThat(kpiResult.getPdpContextCutoffRatio(), is(ZERO));
        assertThat(kpiResult.getDetachSuccessRate(), is(ZERO));
        assertThat(kpiResult.getServiceRequestFailureRatio(), is(ZERO));
        assertThat(kpiResult.getPagingFailureRatio(), is(ZERO));
        assertThat(kpiResult.getPagingattemptsperSubs(), is(ZERO));
        assertThat(kpiResult.getImpactedSubscribers(), is("0"));
        assertThat(kpiResult.getAttachSuccessRateLTE(), is(ZERO));
        assertThat(kpiResult.getPdnConnectionSuccessRate(), is(ZERO));
        assertThat(kpiResult.getImpactedSubscribersLTE(), is("0"));
        assertThat(kpiResult.getBearerActivationSuccessRate(), is(ZERO));
        assertThat(kpiResult.getUeInitiatedServiceRequestFailureRatioLTE(), is(ZERO));
        assertThat(kpiResult.getPagingFailureRatioLTE(), is(ZERO));
        assertThat(kpiResult.getTrackingAreaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getInterMMETrackingAreaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getX2basedhandover(), is(ZERO));
        assertThat(kpiResult.getX2basedHOwithoutSGWrelocation(), is(ZERO));
        assertThat(kpiResult.getX2basedHOwithSGWrelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedhandover(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandwithMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandwithoutMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandMMErelocation(), is(ZERO));

    }

    protected void validateSingleResultForManyEvents(final KPIAnalysisResult kpiResult) throws Exception {

        assertThat(kpiResult.getAttachSuccessRate(), is(ZERO));
        assertThat(kpiResult.getPdpContextActSR(), is(ZERO));
        assertThat(kpiResult.getRaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getInterSGSN_MMERAUpdateSR(), is(ZERO));
        assertThat(kpiResult.getPdpContextCutoffRatio(), is(ZERO));
        assertThat(kpiResult.getDetachSuccessRate(), is(ZERO));
        assertThat(kpiResult.getServiceRequestFailureRatio(), is(ZERO));
        assertThat(kpiResult.getPagingFailureRatio(), is(ZERO));
        assertThat(kpiResult.getPagingattemptsperSubs(), is(ZERO));
        assertThat(kpiResult.getImpactedSubscribers(), is("0"));
        assertThat(kpiResult.getAttachSuccessRateLTE(), is(ZERO));
        assertThat(kpiResult.getPdnConnectionSuccessRate(), is(ZERO));
        assertThat(kpiResult.getImpactedSubscribersLTE(), is("3"));
        assertThat(kpiResult.getBearerActivationSuccessRate(), is(ZERO));
        assertThat(kpiResult.getUeInitiatedServiceRequestFailureRatioLTE(), is(73.33));
        assertThat(kpiResult.getPagingFailureRatioLTE(), is(37.50));
        assertThat(kpiResult.getTrackingAreaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getInterMMETrackingAreaUpdateSuccessRate(), is(ZERO));
        assertThat(kpiResult.getX2basedhandover(), is(ZERO));
        assertThat(kpiResult.getX2basedHOwithoutSGWrelocation(), is(ZERO));
        assertThat(kpiResult.getX2basedHOwithSGWrelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedhandover(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandwithMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandwithoutMMErelocation(), is(ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandMMErelocation(), is(ZERO));

    }

    private void insertRowIntoAggTable(final String table, final int eventId, final String eventSrcName,
            final int pagingAttempts, final int noErrors, final int noSuccesses, final Integer eventSubType,
            final String dateTime, final int netInitDeactivates, final int totalErrSubs) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVENT_SOURCE_NAME, eventSrcName);
        values.put(NO_OF_PAGING_ATTEMPTS, pagingAttempts);
        values.put(NO_OF_ERRORS, noErrors);
        values.put(NO_OF_SUCCESSES, noSuccesses);
        values.put(EVENT_SUBTYPE_ID, eventSubType);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_NET_INIT_DEACTIVATES, netInitDeactivates);
        values.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, totalErrSubs);
        insertRow(table, values);
        insertRowOnSpecificConnection(anotherConnection, table, values);
    }

    protected void insertRowIntoRawTable(final String table, final int eventId, final long imsi,
            final String eventSrcName, final int pagingAttempts, final Integer deactivationTrigger,
            final Integer eventSubType, final int tac, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(IMSI, imsi);
        values.put(EVENT_SOURCE_NAME, eventSrcName);
        values.put(PAGING_ATTEMPTS, pagingAttempts);
        values.put(DEACTIVATION_TRIGGER, deactivationTrigger);
        values.put(EVENT_SUBTYPE_ID, eventSubType);
        values.put(TAC, tac);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
        insertRowOnSpecificConnection(anotherConnection, table, values);
    }
}
