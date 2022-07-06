package com.ncr.backstage.cost_insights.enums;

/**
 * Enum to represent the column names that get returned after querying the
 * BigQuery billing export table
 */
public enum QueryReturnColumns {

    PROJECT_NAME("project_name"),
    SERVICE_DESCRIPTION("service_description"),
    SKU_DESCRIPTION("sku_description"),
    USAGE_START_DAY("usage_start_day"),
    USAGE_START_DAY_EPOCH_SECONDS("usage_start_day_epoch_seconds"),
    SUM_COST("sum_cost");

    /** Store a lowercase label that corresponds to the return columns */
    public final String label;

    private QueryReturnColumns(String label) {
        this.label = label;
    }

    /** Override to return label by default */
    @Override
    public String toString() {
        return this.label;
    }
}
