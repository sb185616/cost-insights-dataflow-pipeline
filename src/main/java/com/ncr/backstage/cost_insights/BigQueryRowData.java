package com.ncr.backstage.cost_insights;

import java.math.BigDecimal;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.google.api.services.bigquery.model.TableRow;

/**
 * Class to represent row data returned by BigQuery after being queried by the
 * pipeline
 */
class BigQueryRowData {

    @DefaultCoder(AvroCoder.class)
    static class RowData {
        String project_name;
        String service_description;
        String sku_description;
        String usage_start_day;
        Long usage_start_day_epoch_seconds;
        BigDecimal sum_cost;

        public static RowData fromTableRow(TableRow row) {
            RowData data = new RowData();

            data.project_name = (String) row.get(QueryReturnColumns.PROJECT_NAME.label);
            data.service_description = (String) row.get(QueryReturnColumns.SERVICE_DESCRIPTION.label);
            data.sku_description = (String) row.get(QueryReturnColumns.SKU_DESCRIPTION.label);
            data.usage_start_day = (String) row.get(QueryReturnColumns.USAGE_START_DAY.label);
            data.usage_start_day_epoch_seconds = (Long) row.get(QueryReturnColumns.USAGE_START_DAY_EPOCH_SECONDS.label);
            data.sum_cost = new BigDecimal((String) row.get(QueryReturnColumns.SUM_COST.label));

            return data;
        }
    }
}
