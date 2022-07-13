package com.ncr.backstage.cost_insights;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableReference;
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
        ValueProvider<String> getBigtableProjectId();

        void setBigtableProjectId(ValueProvider<String> bigtableProjectId);

        @Description("The Bigtable instance ID")
        ValueProvider<String> getBigtableInstanceId();

        void setBigtableInstanceId(ValueProvider<String> bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        ValueProvider<String> getBigtableTableId();

        void setBigtableTableId(ValueProvider<String> bigtableTableId);

        @Description("The BigQuery project ID, this can be different than your Dataflow project")
        ValueProvider<String> getBigQueryProjectId();

        void setBigQueryProjectId(ValueProvider<String> bigQueryProjectId);

        @Description("The BigQuery dataset ID")
        ValueProvider<String> getBigQueryDatasetId();

        void setBigQueryDatasetId(ValueProvider<String> bigQueryDatasetId);

        @Description("The BigQuery table ID in the Dataset.")
        ValueProvider<String> getBigQueryTableId();

        void setBigQueryTableId(ValueProvider<String> bigQueryTableId);

        @Description("Set if the rows returned from querying BigQuery are to be written to a text file")
        @Default.Boolean(false)
        ValueProvider<Boolean> getWriteBQRowsToTextFile();

        void setWriteBQRowsToTextFile(ValueProvider<Boolean> writeBQRowsToTextFile);

        @Default.String("gs://backstage-dataflow-pipeline/bigquery_read_output")
        ValueProvider<String> getOutputLocation();

        void setOutputLocation(ValueProvider<String> outputLocation);

        @Description("Billing export scan window")
        @Default.Integer(2)
        ValueProvider<Integer> getDayDelta();

        void setDayDelta(ValueProvider<Integer> dayDelta);

        @Description("Date for which costs are to be aggregated")
        ValueProvider<LocalDate> getAggregationDate();

        void setAggregationDate(ValueProvider<LocalDate> date);
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

        TableReference input = new TableReference().setProjectId(options.getBigQueryProjectId().get())
                .setDatasetId(options.getBigQueryDatasetId().get()).setTableId(options.getBigQueryTableId().get());
        String output = String.format("%s.%s.%s", options.getBigtableProjectId(), options.getBigtableInstanceId(),
                options.getBigtableTableId());

        LOG.info("Using {} as input BigQuery table\nUsing {} as the output Bigtable table", input.toString(), output);

        BigQueryReader bigQueryReader = new BigQueryReader(pipeline, input);
        PCollection<RowData> rowsRetrieved = bigQueryReader.directReadWithSQLQuery();

        if (options.getWriteBQRowsToTextFile().get()) {
            rowsRetrieved.apply(
                    MapElements.via(new SimpleFunction<RowData, String>() {
                        @Override
                        public String apply(RowData row) {
                            return row.toString();
                        }
                    })).apply("Write BQ rows to text file", TextIO.write().to(options.getOutputLocation()));
        }

        BigtableWriter bigtableWriter = new BigtableWriter(pipeline);
        PCollection<RowData> rowData = bigtableWriter.createNeededColumnFamilies(rowsRetrieved);

        bigtableWriter.applyRowMutations(rowData);

        LOG.info("Running pipeline!");
        pipeline.run().waitUntilFinish();

    }
}
