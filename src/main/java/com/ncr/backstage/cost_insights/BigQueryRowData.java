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

    /** Name to be used if project name is missing from a table row */
    private static final String NULL_PROJECT = "null-project";
    /** Max length for a column family identifier */
    private static final Integer COLUMN_FAMILY_ID_MAX_LENGTH = 64;

    @DefaultCoder(AvroCoder.class)
    static class RowData {
        String project_name;
        String service_description;
        String sku_description;
        String usage_start_day;
        Long usage_start_day_epoch_seconds;
        Double sum_cost;

        /**
         * Creates a RowData object from a TableRow
         * 
         * @param row is a TableRow object
         */
        public static RowData fromTableRow(TableRow row) {
            RowData data = new RowData();

            Object project_name_object = row.get(QueryReturnColumns.PROJECT_NAME.label);
            data.project_name = project_name_object == null ? NULL_PROJECT : (String) project_name_object;
            data.project_name = data.project_name.strip();
            data.service_description = formatColumnFamilyName(
                    (String) row.get(QueryReturnColumns.SERVICE_DESCRIPTION.label));
            data.sku_description = ((String) row.get(QueryReturnColumns.SKU_DESCRIPTION.label)).strip();
            data.usage_start_day = (String) row.get(QueryReturnColumns.USAGE_START_DAY.label);
            data.usage_start_day_epoch_seconds = Long
                    .parseLong((String) row.get(QueryReturnColumns.USAGE_START_DAY_EPOCH_SECONDS.label));
            data.sum_cost = (Double) row.get(QueryReturnColumns.SUM_COST.label);
            return data;
        }

        /**
         * Takes in a cloud service name, formats it and returns it to be used as a
         * column family name
         * 
         * @param name is the unformatted service name
         * @return formats the input name and returns it
         */
        private static String formatColumnFamilyName(String name) {
            String formatted = name
                    .strip() // remove leading or trailing whitespace
                    .replaceAll("\\(.+?\\)", "") // remove parentheses and the string contained
                    .strip() // remove leading or trailing whitespace from resulting string
                    .replace(' ', '_') // replace whitespace between words with underscore
                    .replaceAll("(\\W|^_)", ""); // remove special characters except underscore
            return formatted.substring(0, Math.min(formatted.length(), COLUMN_FAMILY_ID_MAX_LENGTH)); // cut string down
                                                                                                      // to 64
                                                                                                      // characters if
                                                                                                      // it is longer
        }

        /**
         * Takes in a RowData object, and returns a cloned RowData object
         * 
         * @param input is a RowData object
         * @return returns a cloned RowData object
         */
        public static RowData fromRowData(RowData input) {
            RowData output = new RowData();
            output.project_name = input.project_name;
            output.service_description = input.service_description;
            output.sku_description = input.sku_description;
            output.usage_start_day = input.usage_start_day;
            output.usage_start_day_epoch_seconds = input.usage_start_day_epoch_seconds;
            output.sum_cost = input.sum_cost;
            return output;
        }

        @Override
        public String toString() {
            return String.format("Project: %s | Service: %s | Service SKU: %s | Usage Day: %s | Cost Incurred : %s",
                    this.project_name, this.service_description, this.sku_description, this.usage_start_day,
                    this.sum_cost);
        }
    }
}
