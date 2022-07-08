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

    private static final String NULL_PROJECT = "null-project";

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

            Object project_name_object = row.get(QueryReturnColumns.PROJECT_NAME.label);
            data.project_name = project_name_object == null ? NULL_PROJECT : (String) project_name_object;
            data.project_name = data.project_name.strip();
            data.service_description = ((String) row.get(QueryReturnColumns.SERVICE_DESCRIPTION.label)).strip();
            data.service_description = data.service_description.replace('/', '_').replace(' ', '_');
            data.sku_description = ((String) row.get(QueryReturnColumns.SKU_DESCRIPTION.label)).strip();
            data.usage_start_day = (String) row.get(QueryReturnColumns.USAGE_START_DAY.label);
            data.usage_start_day_epoch_seconds = Long
                    .parseLong((String) row.get(QueryReturnColumns.USAGE_START_DAY_EPOCH_SECONDS.label));
            // data.sum_cost = (Double) row.get(QueryReturnColumns.SUM_COST.label); // TODO
            // change back
            data.sum_cost = Double.parseDouble((String) row.get(QueryReturnColumns.SUM_COST.label));
            return data;
        }

        @Override
        public String toString() {
            return String.format("%s incurred %s on %s for service %s-%s", this.project_name, this.sum_cost,
                    this.usage_start_day,
                    this.service_description, this.sku_description);
        }
    }
}
