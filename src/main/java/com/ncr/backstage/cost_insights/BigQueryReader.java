package com.ncr.backstage.cost_insights;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.api.services.bigquery.model.TableReference;
import com.ncr.backstage.cost_insights.BigQueryRowData.RowData;

public class BigQueryReader {

    /* The pipeline for which the BigQuery reads are being performed */
    private final Pipeline pipeline;
    /* The BigQuery table which is to be read from */
    private final TableReference tableReference;
    /* Number of days the aggregation date is behind the current day */
    private final int dayDelta;
    /* List of fields to be read from the BigQuery Table */
    // private final List<String> desiredFields;

    public BigQueryReader(Pipeline pipeline, TableReference tableReference, int dayDelta) {
        this.pipeline = pipeline;
        this.tableReference = tableReference;
        this.dayDelta = dayDelta;
        // this.desiredFields = new ArrayList<String>();
        // this.setFieldsToRead();
    }

    // /**
    // * Sets fields to read from the BigQuery Table. Fetches fields from the
    // * BigQueryColumns Enum
    // */
    // private void setFieldsToRead() {
    // Arrays.asList(BigQueryColumns.class.getEnumConstants()).forEach(x ->
    // this.desiredFields.add(x.label));
    // }

    // public PCollection<?> directRead() {
    // PCollection<?> rows = this.pipeline.apply("Read from BigQuery Table",
    // BigQueryIO.readTableRows().from(this.tableReference).withMethod(Method.DIRECT_READ)
    // .withSelectedFields(this.desiredFields)
    // .withRowRestriction(String.format("%s > 0", BigQueryColumns.COST)) // where
    // cost > 0
    // // where usage start time starts with the desired date
    // .withRowRestriction(String.format("STARTS_WITH(%s, '%s')",
    // BigQueryColumns.USAGE_START_TIME,
    // this.dateString)));
    // return rows;
    // }

    /**
     * Reads rows from the given BigQuery Table using a query string
     * 
     * @return Returns a PCollection of rows
     */
    public PCollection<RowData> directReadWithSQLQuery() {
        String tableReferenceString = String.format("`%s:%s.%s`", this.tableReference.getProjectId(),
                this.tableReference.getDatasetId(), this.tableReference.getTableId());
        ZonedDateTime startOfToday = LocalDate.now().atStartOfDay(ZoneOffset.UTC);

        String query = String.format("SELECT\n" +
                "  %1$s AS %2$s,\n" + // project.name, project_name
                "  %3$s AS %4$s,\n" + // service.description, service_description
                "  %5$s AS %6$s,\n" + // sku.description, sku_description
                "  FORMAT_TIMESTAMP('%F', %7$s, 'UTC') AS %8$s,\n" + // usage_start_time, usage_start_day
                "  UNIX_SECONDS(TIMESTAMP_TRUNC(%7$s, DAY, 'UTC')) AS %9$s,\n" + // usage_start_time,
                // usage_start_day_epoch_seconds
                "  SUM(%10$s) AS %11$s" + // cost, sum_cost
                "FROM\n" +
                "  %12$s\n" + // tablereferenceString declared above
                "WHERE\n" +
                "  %13$s BETWEEN TIMESTAMP(%14$s)\n" + // partitiontime, aggregation date
                "  AND TIMESTAMP(%15$s)\n" + // aggregation date + some days
                "  AND %10$s > 0\n" + // cost
                "  AND STARTS_WITH(STRING(%7$s, 'UTC'), %14$s)\n" + // usage_start_time, aggregation date
                "GROUP BY\n" +
                "  %2$s,\n" +
                "  %4$s,\n" +
                "  %6$s,\n" +
                "  %8$s,\n" +
                "  %9$s",

                BigQueryColumns.PROJECT_NAME,
                QueryReturnColumns.PROJECT_NAME,

                BigQueryColumns.SERVICE_DESCRIPTION,
                QueryReturnColumns.SERVICE_DESCRIPTION,

                BigQueryColumns.SKU_DESCRIPTION,
                QueryReturnColumns.SKU_DESCRIPTION,

                BigQueryColumns.USAGE_START_TIME,
                QueryReturnColumns.USAGE_START_DAY,
                QueryReturnColumns.USAGE_START_DAY_EPOCH_SECONDS,

                BigQueryColumns.COST,
                QueryReturnColumns.SUM_COST,

                tableReferenceString,
                BigQueryColumns.PARTITION_TIME,
                startOfToday.minusDays(this.dayDelta).toLocalDate().toString(),
                startOfToday.toString());

        PCollection<RowData> rows = this.pipeline.apply("Read from BigQuery table with query string",
                BigQueryIO.readTableRows().fromQuery(query).usingStandardSql().withMethod(Method.DIRECT_READ))
                .apply("Transform returned table rows into Java Objects",
                        MapElements.into(TypeDescriptor.of(RowData.class)).via(RowData::fromTableRow));
        return rows;
    }

}
