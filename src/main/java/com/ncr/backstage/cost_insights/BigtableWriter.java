package com.ncr.backstage.cost_insights;

import java.nio.ByteBuffer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
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
     * Takes in a PCollection of RowData objects, maps them to Mutation objects and
     * applies them to the output
     * Bigtable table
     * 
     * @param mutations is the PCollection of mutations passed in
     */
    public void applyRowMutations(PCollection<RowData> rows) {
        CloudBigtableTableConfiguration bigtableTableConfig = new CloudBigtableTableConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();
        rows.apply(MapElements.via(ROWDATA_MUTATION)).apply(CloudBigtableIO.writeToTable(bigtableTableConfig));
    }

    /**
     * Transforms a RowData object to a Mutation
     */
    static final SimpleFunction<RowData, Mutation> ROWDATA_MUTATION = new SimpleFunction<RowData, Mutation>() {

        @Override
        public Mutation apply(RowData data) {
            final byte[] ROW = (data.project_name + (((1L << 63) - 1) - data.usage_start_day_epoch_seconds)).getBytes();
            final byte[] FAMILY = data.service_description.getBytes();
            final byte[] QUALIFIER = data.sku_description.getBytes();
            final Long TIMESTAMP = data.usage_start_day_epoch_seconds;
            final byte[] VALUE = ByteBuffer.allocate(8).putDouble(data.sum_cost).array();
            Mutation mutation = new Put(ROW).addColumn(FAMILY, QUALIFIER, TIMESTAMP, VALUE);
            return mutation;
        }
    };

}
