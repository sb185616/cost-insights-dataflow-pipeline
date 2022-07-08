package com.ncr.backstage.cost_insights;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.repackaged.com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;
import com.ncr.backstage.cost_insights.BigQueryRowData.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that runs the pipeline.
 * Reads billing info from given BigQuery tables, aggregates the costs by day,
 * and exports the transformed data to the given Bigtable table
 */
public class PipelineRunner {

    /** Pipeline options required */
    public interface DataflowPipelineOptions extends PipelineOptions {
        @Description("The Bigtable project ID, this can be different than your Dataflow project")
        String getBigtableProjectId();

        void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        String getBigtableInstanceId();

        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        String getBigtableTableId();

        void setBigtableTableId(String bigtableTableId);

        @Description("The BigQuery project ID, this can be different than your Dataflow project")
        String getBigQueryProjectId();

        void setBigQueryProjectId(String bigQueryProjectId);

        @Description("The BigQuery dataset ID")
        String getBigQueryDatasetId();

        void setBigQueryDatasetId(String bigQueryDatasetId);

        @Description("The BigQuery table ID in the Dataset.")
        String getBigQueryTableId();

        void setBigQueryTableId(String bigQueryTableId);

        @Default.String("gs://backstage-dataflow-pipeline/output")
        String getOutputLocation();

        void setOutputLocation(String outputLocation);

        @Description("Billing export scan window")
        @Default.Integer(2)
        int getDayDelta();

        void setDayDelta(int dayDelta);

        @Description("Date for which costs are to be aggregated")
        LocalDate getAggregationDate();

        void setAggregationDate(LocalDate date);
    }

    /* Logger */
    private static final Logger LOG = LoggerFactory.getLogger(PipelineRunner.class);

    /**
     * Reads in a JSON file and returns a list of TableReference objects that point
     * to the input BigQuery tables
     * 
     * @param filepath is the path for the JSON file
     * @return list of table references
     */
    private static TableReference getTableReference(String filepath) throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(filepath));
        TableReference reference = new Gson().fromJson(reader, new TypeToken<TableReference>() {
        }.getType());
        reader.close();
        return reference;
    }

    /**
     * Sets up and runs the pipeline
     * 
     * @param args command line args, used to pass in Bigtable table references /
     *             BigQuery table reference JSON file path / day delta for cost
     *             aggregation
     * @throws Exception IOException thrown if JSON file containing BQ table data
     *                   cannot be read
     */
    public static void main(final String[] args) throws Exception {
        LOG.info("Creating Dataflow Aggregation Pipeline!");
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DataflowPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        TableReference input = new TableReference().setProjectId(options.getBigQueryProjectId())
                .setDatasetId(options.getBigQueryDatasetId()).setTableId(options.getBigQueryTableId());
        String output = String.format("%s.%s.%s", options.getBigtableProjectId(), options.getBigtableInstanceId(),
                options.getBigtableTableId());

        LOG.info("Using {} as input BigQuery table\nUsing {} as the output Bigtable table", input.toString(), output);

        // BigQueryReader bigQueryReader = new BigQueryReader(pipeline, input);
        // PCollection<RowData> rowsRetrieved = bigQueryReader.directReadWithSQLQuery();

        PCollection<RowData> rowsRetrieved = pipeline.apply(TestUtil.getValues("testData/tableRows/allrows"))
                .apply(MapElements.into(TypeDescriptor.of(RowData.class)).via(RowData::fromTableRow));

        rowsRetrieved.apply(MapElements.via(new FormatAsTextFn()))
                .apply("Write BQ rows to text file", TextIO.write().to(options.getOutputLocation()));

        BigtableWriter bigtableWriter = new BigtableWriter(pipeline);
        // PCollection<RowData> rowData =
        // bigtableWriter.createNeededColumnFamilies(rowsRetrieved);

        // bigtableWriter.applyRowMutations(rowsRetrieved);

        LOG.info("Running pipeline!");
        pipeline.run().waitUntilFinish();

    }

    /** For testing */ // TODO remove
    public static class FormatAsTextFn extends SimpleFunction<RowData, String> {
        @Override
        public String apply(RowData input) {
            return input.toString();
        }
    }
}
