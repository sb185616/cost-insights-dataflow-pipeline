package com.ncr.backstage.cost_insights.enums;

/**
 * Enum to represent the columns that we wish to read from the BigQuery table
 */
public enum BigQueryColumns {

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