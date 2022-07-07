package com.ncr.backstage.cost_insights;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.google.api.services.bigquery.model.TableRow;
import com.ncr.backstage.cost_insights.enums.QueryReturnColumns;

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
        Double sum_cost;

        public static RowData fromTableRow(TableRow row) {
            RowData data = new RowData();

            data.project_name = (String) row.get(QueryReturnColumns.PROJECT_NAME.label);
            data.service_description = (String) row.get(QueryReturnColumns.SERVICE_DESCRIPTION.label);
            data.sku_description = (String) row.get(QueryReturnColumns.SKU_DESCRIPTION.label);
            data.usage_start_day = (String) row.get(QueryReturnColumns.USAGE_START_DAY.label);
            data.usage_start_day_epoch_seconds = (Long) row.get(QueryReturnColumns.USAGE_START_DAY_EPOCH_SECONDS.label);
            data.sum_cost = ((Double) row.get(QueryReturnColumns.SUM_COST.label));

            return data;
        }

        @Override
        public String toString() {
            return String.format("Costs incurred by %s on %s: %s-%s = %s", this.project_name, this.usage_start_day,
                    this.service_description, this.sku_description, this.sum_cost);
        }
    }
}
