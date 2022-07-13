package com.ncr.backstage.cost_insights;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ncr.backstage.cost_insights.BigQueryRowData.RowData;
import com.ncr.backstage.cost_insights.PipelineRunner.DataflowPipelineOptions;
import com.ncr.backstage.cost_insights.enums.BigQueryColumns;
import com.ncr.backstage.cost_insights.enums.QueryReturnColumns;

/**
 * Class that defines steps required to read data from the BigQuery billing
 * export table
 */
public class BigQueryReader {

    /* Logger */
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReader.class);
    /* The pipeline for which the BigQuery reads are being performed */
    private final Pipeline pipeline;

    /**
     * Constructor
     * 
     * @param pipeline is the apache beam pipeline being used
     */
    public BigQueryReader(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * Reads rows from the given BigQuery Table using a query string
     * 
     * @return Returns a PCollection of rows
     */
    public PCollection<RowData> directReadWithSQLQuery() {
        DataflowPipelineOptions options = pipeline.getOptions().as(DataflowPipelineOptions.class);
        String tableReferenceString = String.format("`%s.%s.%s`", options.getBigQueryProjectId(),
                options.getBigQueryDatasetId(), options.getBigQueryTableId());
        ZonedDateTime startOfToday = options.getAggregationDate().atStartOfDay(ZoneOffset.UTC);
        String aggregationDate = startOfToday.toLocalDate().toString();
        String billingExportScanWindowEnd = startOfToday.plusDays(options.getDayDelta()).toLocalDate().toString();

        String query = String.format("SELECT\n" +
                "  %1$s AS %2$s,\n" + // project.name, project_name
                "  %3$s AS %4$s,\n" + // service.description, service_description
                "  %5$s AS %6$s,\n" + // sku.description, sku_description
                "  FORMAT_TIMESTAMP('%%F', %7$s, 'UTC') AS %8$s,\n" + // usage_start_time, usage_start_day
                "  UNIX_SECONDS(TIMESTAMP_TRUNC(%7$s, DAY, 'UTC')) AS %9$s,\n" + // usage_start_time,
                // usage_start_day_epoch_seconds
                "  SUM(%10$s) AS %11$s\n" + // cost, sum_cost
                "FROM\n" +
                "  %12$s\n" + // tablereferenceString declared above
                "WHERE\n" +
                "  %13$s BETWEEN TIMESTAMP('%14$s')\n" + // partitiontime, aggregation date
                "  AND TIMESTAMP('%15$s')\n" + // aggregation date + some days
                "  AND %10$s > 0\n" + // cost
                "  AND STARTS_WITH(STRING(%7$s, 'UTC'), '%14$s')\n" + // usage_start_time, aggregation date
                "GROUP BY\n" +
                "  %2$s,\n" + // project_name
                "  %4$s,\n" + // service_description
                "  %6$s,\n" + // sku_description
                "  %8$s,\n" + // usage_start_day
                "  %9$s", // usage_start_day_epoch_seconds

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
                aggregationDate,
                billingExportScanWindowEnd);

        LOG.info("Setting aggregatiion date of {}, with a export scan window till the beginning of {}", aggregationDate,
                billingExportScanWindowEnd);
        LOG.info("Query to be run on {}:\n{}", tableReferenceString, query);

        PCollection<RowData> rows = this.pipeline
                .apply("Read from BigQuery table with query string",
                        BigQueryIO.readTableRows()
                                .fromQuery(query)
                                .usingStandardSql()
                                .withMethod(Method.DIRECT_READ))
                .apply("Transform BigQuery table rows retrieved into Java RowData Objects",
                        MapElements.into(TypeDescriptor.of(RowData.class))
                                .via(RowData::fromTableRow));
        return rows;
    }

}
