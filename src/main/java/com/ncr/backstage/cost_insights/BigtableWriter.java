package com.ncr.backstage.cost_insights;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.ncr.backstage.cost_insights.BigQueryRowData.RowData;
import com.ncr.backstage.cost_insights.PipelineRunner.DataflowPipelineOptions;

public class BigtableWriter {

    /* Logger */
    private static final Logger LOG = LoggerFactory.getLogger(BigtableWriter.class);
    /* The pipeline which performs the writes to Bigtable */
    private final Pipeline pipeline;
    /* String reference to the Bigtable table */
    private DataflowPipelineOptions options;

    /**
     * Constructor
     * 
     * @param pipeline the pipeline
     * @param options
     */
    public BigtableWriter(Pipeline pipeline, DataflowPipelineOptions options) {
        this.pipeline = pipeline;
        this.options = options;
    }

    /**
     * Takes in row data returned after querying the BigQuery billing export table.
     * Checks the output Bigtable to see if required column families exist, and
     * creates them if they don't exist.
     * 
     * @param rows are the rows returned after querying BigQuery
     * @return returns the mutations needed to write the information to Bigtable
     */
    public PCollection<Mutation> getRequiredMutations(PCollection<RowData> rows) {
        // TODO
        return null;
    }

    /**
     * Takes in a PCollection of mutation objects and applies them to the output
     * Bigtable table
     * 
     * @param mutations is the PCollection of mutations passed in
     */
    public void applyRowMutations(PCollection<Mutation> mutations) {
        CloudBigtableTableConfiguration bigtableTableConfig = new CloudBigtableTableConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();
        mutations.apply(CloudBigtableIO.writeToTable(bigtableTableConfig));
    }

    static class CheckBigtableColumns extends PTransform<PCollection<RowData>, PCollection<Mutation>> {

        @Override
        public PCollection<Mutation> expand(PCollection<RowData> input) {
            // TODO Auto-generated method stub
            return null;
        }
    }

}
