package com.ncr.backstage.cost_insights;

/**
 * Enum to represent the columns that we wish to read from the BigQuery table
 */
public enum BigQueryColumns {

    PROJECT("project"),
    SERVICE("service"),
    SKU("sku"),
    USAGE_START_TIME("usage_start_time"),
    COST("cost");

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
