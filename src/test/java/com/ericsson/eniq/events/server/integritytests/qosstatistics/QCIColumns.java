/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Some helper methods for tests on QCI/QOS queries
 * @author eemecoy
 *
 */
public class QCIColumns {

    public static Collection<String> getQCIColumns() {
        final Collection<String> qciColumns = new ArrayList<String>();
        qciColumns.add(QCI_ERR_1);
        qciColumns.add(QCI_ERR_2);
        qciColumns.add(QCI_ERR_3);
        qciColumns.add(QCI_ERR_4);
        qciColumns.add(QCI_ERR_5);
        qciColumns.add(QCI_ERR_6);
        qciColumns.add(QCI_ERR_7);
        qciColumns.add(QCI_ERR_8);
        qciColumns.add(QCI_ERR_9);
        qciColumns.add(QCI_ERR_10);
        qciColumns.add(QCI_SUC_1);
        qciColumns.add(QCI_SUC_2);
        qciColumns.add(QCI_SUC_3);
        qciColumns.add(QCI_SUC_4);
        qciColumns.add(QCI_SUC_5);
        qciColumns.add(QCI_SUC_6);
        qciColumns.add(QCI_SUC_7);
        qciColumns.add(QCI_SUC_8);
        qciColumns.add(QCI_SUC_9);
        qciColumns.add(QCI_SUC_10);
        return qciColumns;
    }

    public static Map<String, Object> getDefaultQCIValues() {
        final Map<String, Object> valuesForAggErrTable = new HashMap<String, Object>();
        valuesForAggErrTable.put(QCI_ERR_1, 0);
        valuesForAggErrTable.put(QCI_ERR_2, 0);
        valuesForAggErrTable.put(QCI_ERR_3, 0);
        valuesForAggErrTable.put(QCI_ERR_4, 0);
        valuesForAggErrTable.put(QCI_ERR_5, 0);
        valuesForAggErrTable.put(QCI_ERR_6, 0);
        valuesForAggErrTable.put(QCI_ERR_7, 0);
        valuesForAggErrTable.put(QCI_ERR_8, 0);
        valuesForAggErrTable.put(QCI_ERR_9, 0);
        valuesForAggErrTable.put(QCI_ERR_10, 0);
        valuesForAggErrTable.put(QCI_SUC_1, 0);
        valuesForAggErrTable.put(QCI_SUC_2, 0);
        valuesForAggErrTable.put(QCI_SUC_3, 0);
        valuesForAggErrTable.put(QCI_SUC_4, 0);
        valuesForAggErrTable.put(QCI_SUC_5, 0);
        valuesForAggErrTable.put(QCI_SUC_6, 0);
        valuesForAggErrTable.put(QCI_SUC_7, 0);
        valuesForAggErrTable.put(QCI_SUC_8, 0);
        valuesForAggErrTable.put(QCI_SUC_9, 0);
        valuesForAggErrTable.put(QCI_SUC_10, 0);
        return valuesForAggErrTable;
    }

}
