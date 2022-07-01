package com.ncr.backstage.cost_insights;

/**
 * Enum to represent the columns that we wish to read from the BigQuery table
 */
enum BigQueryColumns {

    PROJECT_NAME("project.name"),
    SERVICE_DESCRIPTION("service.description"),
    SKU_DESCRIPTION("sku.description"),
    USAGE_START_TIME("usage_start_time"),
    COST("cost"),
    PARTITION_TIME("_PARTITIONTIME");

    /** Store a lowercase label that corresponds to the BigQuery columns */
    public final String label;

    private BigQueryColumns(String label) {
        this.label = label;
    }

    /** Override to return label by default */
    @Override
    public String toString() {
        return this.label;
    }
}

enum QueryReturnColumns {

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
