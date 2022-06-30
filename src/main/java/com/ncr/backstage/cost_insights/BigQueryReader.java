package com.ncr.backstage.cost_insights;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.values.PCollection;

public class BigQueryReader {

    /** The pipeline for which the BigQuery reads are being performed */
    private final Pipeline pipeline;
    /** The BigQuery table which is to be read from */
    private final BigQueryTableRef bigQueryTableRef;
    /** Unix timestamp for the date for which costs are to be aggregated */
    private final long timestamp;
    /** YYYY-MM-DD date string for the date for which costs are to be aggregated */
    private final String dateString;
    /** List of fields to be read from the BigQuery Table */
    private final List<String> desiredFields;

    public BigQueryReader(Pipeline pipeline, BigQueryTableRef tableRef) {
        this.pipeline = pipeline;
        this.bigQueryTableRef = tableRef;
        ZonedDateTime aggregationDate = LocalDate.now().atStartOfDay(ZoneOffset.UTC).minusDays(2);
        this.dateString = aggregationDate.toLocalDate().toString();
        this.timestamp = aggregationDate.toEpochSecond();
        this.desiredFields = new ArrayList<String>();
        this.setFieldsToRead();
    }

    /**
     * Sets fields to read from the BigQuery Table. Fetches fields from the
     * BigQueryColumns Enum
     */
    private void setFieldsToRead() {
        Arrays.asList(BigQueryColumns.class.getEnumConstants()).forEach(x -> this.desiredFields.add(x.label));
    }

    public PCollection<?> read() {
        PCollection<?> rows = this.pipeline.apply("Read from BigQuery Table",
                BigQueryIO.readTableRows().from(this.bigQueryTableRef.toString()).withMethod(Method.DIRECT_READ)
                        .withSelectedFields(this.desiredFields));
        return rows;
    }

}
