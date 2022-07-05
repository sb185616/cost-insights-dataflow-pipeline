package com.ncr.backstage.cost_insights;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
        String getBigtableProjectId();

        void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        String getBigtableInstanceId();

        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        String getBigtableTableId();

        void setBigtableTableId(String bigtableTableId);

        @Description("JSON file containing the table reference to the BigQuery billing export table")
        String getBigQueryTableReferenceJSON();

        void setBigQueryTableReferenceJSON(String bigQueryTableReferenceJSON);

        @Description("Billing export scan window")
        int getDayDelta();

        void setDayDelta(int dayDelta);
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

    public static void main(final String[] args) throws Exception {

        LOG.info("Creating Dataflow Aggregation Pipeline!");
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DataflowPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        TableReference input = PipelineRunner.getTableReference(options.getBigQueryTableReferenceJSON());
        int dayDelta = options.getDayDelta();
        String output = String.format("%s.%s.%s", options.getBigtableProjectId(), options.getBigtableInstanceId(),
                options.getBigtableTableId());

        LOG.info("Using {} as input BigQuery table\n Using {} as the output Bigtable table", input.toString(), output);

        BigQueryReader bigQueryReader = new BigQueryReader(pipeline, input, dayDelta);
        PCollection<RowData> rowData = bigQueryReader.directReadWithSQLQuery();
    }
}
