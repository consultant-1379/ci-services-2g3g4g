package com.ericsson.eniq.events.server.serviceprovider;

import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.templates.utils.TemplateUtils;
import com.ericsson.eniq.events.server.test.common.BaseJMockUnitTest;
import com.ericsson.eniq.events.server.utils.QueryUtils;
import com.ericsson.eniq.events.server.utils.config.ApplicationConfigManager;
import com.ericsson.eniq.events.server.utils.techpacks.TechPackListFactory;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_LTE;
import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_SGEH;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TZ_OFFSET_OF_ZERO;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultipleRankingServiceTest extends BaseJMockUnitTest {

    private MultipleRankingService multipleRankingService;

    TemplateUtils mockedTemplateUtils;

    ApplicationConfigManager applicationConfigManager;

    @Before
    public void setup() {
        multipleRankingService = new MultipleRankingService();

        multipleRankingService.setQueryUtils(new QueryUtils());
        multipleRankingService.setTechPackListFactory(new TechPackListFactory());
        applicationConfigManager = mockery.mock(ApplicationConfigManager.class);
        multipleRankingService.setApplicationConfigManager(applicationConfigManager);
    }

    @Test
    public void testgetServiceSpecificQueryParametersForAPN() {
        expectGetAPNRetentionParameter();
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        requestParameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        final Map<String, QueryParameter> queryParameters = multipleRankingService
                .getServiceSpecificQueryParameters(requestParameters);
        final QueryParameter queryParameterForAPNRetention = queryParameters.get(DATE_FROM_FOR_APN_RETENTION);
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -30);
        final String oneMonthAgo = simpleDateFormat.format(calendar.getTime());
        final String apnRetentionValue = (String) queryParameterForAPNRetention.getValue();
        assertThat(apnRetentionValue.startsWith(oneMonthAgo), is(true));

    }

    private void expectGetAPNRetentionParameter() {
        mockery.checking(new Expectations() {
            {
                one(applicationConfigManager).getAPNRetention();
                will(returnValue(30));
            }
        });

    }

    @Test
    public void testGetApplicableTechPacksForENodeBReturnsJustLTETechpack() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_ENODEB);
        final List<String> applicableTechPacks = multipleRankingService.getApplicableTechPacks(requestParameters);
        assertThat(applicableTechPacks.size(), is(1));
        assertThat(applicableTechPacks.contains(EVENT_E_LTE), is(true));
    }

    @Test
    public void testGetApplicableTechPacksForAPNReturnsSGEHAndLTETechpacks() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        final List<String> applicableTechPacks = multipleRankingService.getApplicableTechPacks(requestParameters);
        assertThat(applicableTechPacks.size(), is(2));
        assertThat(applicableTechPacks.contains(EVENT_E_SGEH), is(true));
        assertThat(applicableTechPacks.contains(EVENT_E_LTE), is(true));
    }

    @Test
    public void testaddColumnsForAggregationQueries_Cell() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_CELL);
        final Map<String, Object> templateParameters = multipleRankingService.getServiceSpecificTemplateParameters(
                requestParameters, null, null);
        final List<String> columnsToSelect = (List<String>) templateParameters.get(COLUMNS_FOR_QUERY);
        assertThat(columnsToSelect.contains(RAT_PARAM), is(true));
        assertThat(columnsToSelect.contains(VENDOR_PARAM_UPPER_CASE), is(true));
        assertThat(columnsToSelect.contains(BSC_SQL_NAME), is(true));
        assertThat(columnsToSelect.contains(CELL_SQL_NAME), is(true));

    }

    @Test
    public void testaddColumnsForAggregationQueries_RNC() {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_RNC);
        final Map<String, Object> templateParameters = multipleRankingService.getServiceSpecificTemplateParameters(
                requestParameters, null, null);
        final List<String> columnsToSelect = (List<String>) templateParameters.get(COLUMNS_FOR_QUERY);
        assertThat(columnsToSelect.contains(RAT_PARAM), is(true));
        assertThat(columnsToSelect.contains(VENDOR_PARAM_UPPER_CASE), is(true));
        assertThat(columnsToSelect.contains(BSC_SQL_NAME), is(true));

    }

    @Test
    public void testaddColumnsForAggregationQueries_BSC() {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_BSC);
        final Map<String, Object> templateParameters = multipleRankingService.getServiceSpecificTemplateParameters(
                requestParameters, null, null);
        final List<String> columnsToSelect = (List<String>) templateParameters.get(COLUMNS_FOR_QUERY);
        assertThat(columnsToSelect.contains(RAT_PARAM), is(true));
        assertThat(columnsToSelect.contains(VENDOR_PARAM_UPPER_CASE), is(true));
        assertThat(columnsToSelect.contains(BSC_SQL_NAME), is(true));

    }

    @Test
    public void testaddColumnsForAggregationQueries_CauseCode() {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_CAUSE_CODE);
        final Map<String, Object> templateParameters = multipleRankingService.getServiceSpecificTemplateParameters(
                requestParameters, null, null);
        final List<String> columnsToSelect = (List<String>) templateParameters.get(COLUMNS_FOR_QUERY);
        assertThat(columnsToSelect.contains(TYPE_CAUSE_PROT_TYPE), is(true));
        assertThat(columnsToSelect.contains(CC_SQL_NAME), is(true));

    }

    @Test
    public void testaddColumnsForAggregationQueries_TAC() {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_TAC);
        final Map<String, Object> templateParameters = multipleRankingService.getServiceSpecificTemplateParameters(
                requestParameters, null, null);
        final List<String> columnsToSelect = (List<String>) templateParameters.get(COLUMNS_FOR_QUERY);
        assertThat(columnsToSelect.contains(TAC_PARAM_UPPER_CASE), is(true));

    }

    @Test
    public void testaddColumnsForAggregationQueries_APN() {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        final Map<String, Object> templateParameters = multipleRankingService.getServiceSpecificTemplateParameters(
                requestParameters, null, null);
        final List<String> columnsToSelect = (List<String>) templateParameters.get(COLUMNS_FOR_QUERY);
        assertThat(columnsToSelect.contains(APN_PARAM_UPPER_CASE), is(true));

    }

}
