/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventanalysis.sgsnmme;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisSGSNMMESummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 *
 */
public class EventAnalysisSummaryWithPreparedRawTablesSGSNMMETest extends
        BaseDataIntegrityTest<EventAnalysisSGSNMMESummaryResult> {

    private EventAnalysisService eventAnalysisService;

    @Before
    public void onSetUp() throws Exception {
        eventAnalysisService = new EventAnalysisService();
        attachDependencies(eventAnalysisService);
        createTemporaryTables();
        populateTemporaryTables();
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSN_4GDataOnly() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(SGSN_PARAM, SAMPLE_MME);
        runQuery(map);
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSNGroup_4GDataOnly() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_MME_GROUP);
        runQuery(map);
    }

    private void runQuery(final MultivaluedMap<String, String> map) throws URISyntaxException, Exception {
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        final String result = getData(eventAnalysisService, map);
        System.out.println(result);
        validateResult(result);
    }

    private void createTemporaryTables() throws Exception {
        createSgehAndLteRawTables();
        createGSNRawTables();
        createGroupTable();
    }

    private void createGroupTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(GROUP_NAME);
        columns.add(EVENT_SOURCE_NAME);
        createTemporaryTable(TEMP_GROUP_TYPE_E_EVNTSRC, columns);
    }

    private void createGSNRawTables() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(DATAVOL_UL);
        columns.add(DATAVOL_DL);
        columns.add(SGSN_NAME);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSN_DT_RAW, columns);
        final Collection<String> columnsForPDPTable = new ArrayList<String>();
        columnsForPDPTable.add(SGSN_NAME_1);
        columnsForPDPTable.add(SGSN_NAME_2);
        columnsForPDPTable.add(SGSN_NAME_3);
        columnsForPDPTable.add(SGSN_NAME_4);
        columnsForPDPTable.add(SGSN_NAME_5);
        columnsForPDPTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSN_DTPDP_RAW, columnsForPDPTable);

    }

    private void createSgehAndLteRawTables() throws Exception {
        final Collection<String> columnsInRawTable = new ArrayList<String>();
        columnsInRawTable.add(EVENT_SOURCE_NAME);
        columnsInRawTable.add(EVENT_ID);
        columnsInRawTable.add(IMSI);
        columnsInRawTable.add(TAC);
        columnsInRawTable.add(DEACTIVATION_TRIGGER);
        columnsInRawTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsInRawTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsInRawTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsInRawTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsInRawTable);
    }

    private void populateTemporaryTables() throws SQLException {
        final String dateTimeId = DateTimeUtilities.getDateTimeMinus3Minutes();

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, ATTACH_IN_4G, SAMPLE_IMSI, dateTimeId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, ATTACH_IN_4G, 999999, dateTimeId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, ATTACH_IN_4G, 231654654, dateTimeId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SAMPLE_MME, ATTACH_IN_4G, SAMPLE_IMSI, dateTimeId);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, PDN_CONNECT_IN_4G, 231654654, dateTimeId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SAMPLE_MME, PDN_CONNECT_IN_4G, SAMPLE_IMSI, dateTimeId);

        insertRowIntoGroupTable(TEMP_GROUP_TYPE_E_EVNTSRC, SAMPLE_MME_GROUP, SAMPLE_MME);

    }

    private void insertRowIntoGroupTable(final String table, final String groupName, final String mme)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, groupName);
        values.put(EVENT_SOURCE_NAME, mme);
        insertRow(table, values);
    }

    private void insertRowIntoRawTable(final String table, final String mme, final int eventId, final long imsi,
            final String dateTimeId) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_SOURCE_NAME, mme);
        values.put(EVENT_ID, eventId);
        values.put(IMSI, imsi);
        values.put(TAC, SAMPLE_TAC);
        values.put(DEACTIVATION_TRIGGER, 0);
        values.put(DATETIME_ID, dateTimeId);
        insertRow(table, values);
    }

    private void validateResult(final String result) throws Exception {
        final List<EventAnalysisSGSNMMESummaryResult> queryResults = getTranslator().translateResult(result,
                EventAnalysisSGSNMMESummaryResult.class);
        assertThat(queryResults.size(), is((2)));

        final EventAnalysisSGSNMMESummaryResult resultForPDNConnectEvent = findQueryResult(PDN_CONNECT_IN_4G,
                queryResults);
        assertThat(resultForPDNConnectEvent.getEventId(), is(PDN_CONNECT_IN_4G));
        assertThat(resultForPDNConnectEvent.getEventIdDesc(), is(L_PDN_CONNECT));
        assertThat(resultForPDNConnectEvent.getErrorCount(), is(1));
        assertThat(resultForPDNConnectEvent.getSuccessCount(), is(1));
        assertThat(resultForPDNConnectEvent.getOccurrences(), is(2));
        assertThat(resultForPDNConnectEvent.getSuccessRatio(),
                is(resultForPDNConnectEvent.calculateExpectedSuccessRatio()));
        assertThat(resultForPDNConnectEvent.getErrorSubscriberCount(), is(1));

        final EventAnalysisSGSNMMESummaryResult resultForLAttachEvent = findQueryResult(ATTACH_IN_4G, queryResults);
        assertThat(resultForLAttachEvent.getEventId(), is(ATTACH_IN_4G));
        assertThat(resultForLAttachEvent.getEventIdDesc(), is(L_ATTACH));
        assertThat(resultForLAttachEvent.getErrorCount(), is(3));
        assertThat(resultForLAttachEvent.getSuccessCount(), is(1));
        assertThat(resultForLAttachEvent.getOccurrences(), is(4));
        assertThat(resultForLAttachEvent.getSuccessRatio(), is(resultForLAttachEvent.calculateExpectedSuccessRatio()));
        assertThat(resultForLAttachEvent.getErrorSubscriberCount(), is(3));

    }

    private EventAnalysisSGSNMMESummaryResult findQueryResult(final int eventId,
            final List<EventAnalysisSGSNMMESummaryResult> queryResults) {
        for (final EventAnalysisSGSNMMESummaryResult result : queryResults) {
            if (result.getEventId() == eventId) {
                return result;
            }
        }
        fail("Could not find expected event for event id : " + eventId);
        return null;
    }
}
