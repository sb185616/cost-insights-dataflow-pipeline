package com.ncr.backstage.cost_insights;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.ncr.backstage.cost_insights.BigQueryRowData.RowData;
import com.ncr.backstage.cost_insights.PipelineRunner.DataflowPipelineOptions;

public class BigtableWriter {

    /* Logger */
    private static final Logger LOG = LoggerFactory.getLogger(BigtableWriter.class);
    /* String reference to the Bigtable table */
    final DataflowPipelineOptions options;

    /**
     * Transform that takes in a PCollection of RowData objects, and returns the
     * exact same PCollection.
     * Within the transform however, it fetches the names of all column families
     * that will be needed to add all the rows to the output table.
     * Then it creates those column families in the output table that do not already
     * exist
     */
    public class CheckColumnFamiliesAndReturnInput
            extends PTransform<PCollection<RowData>, PCollection<RowData>> {

        @Override
        public PCollection<RowData> expand(PCollection<RowData> input) {
            input.apply(MapElements.via(new SimpleFunction<RowData, String>() {
                @Override
                public String apply(RowData data) {
                    return data.service_description;
                }
            })).apply(Distinct.<String>create())
                    .apply(Filter.by(new FilterExistingFamilies(getSetOfExistingFamilies())))
                    .apply(ParDo.of(new CreateColumnFamily(options)));

            return input;
        }

        /**
         * Predicate that takes in a column family name.
         * Constructor takes in a set of Strings representing column families that
         * already exist in the output Bigtable table.
         * Returns true if given column family name does not exist in the set.
         * 
         */
        static class FilterExistingFamilies extends SimpleFunction<String, Boolean> {
            Set<String> existingFamilies;

            FilterExistingFamilies(Set<String> existingFamilies) {
                this.existingFamilies = existingFamilies;
            }

            @Override
            public Boolean apply(String family) {
                return !this.existingFamilies.contains(family);
            }
        }

        /**
         * Helper function that connects to the output Bigtable table, and returns a
         * set of Strings representing all existing column family names
         * 
         * @return a set of strings
         */
        private Set<String> getSetOfExistingFamilies() {
            Set<String> columnFamilies = null;
            try {
                BigtableTableAdminSettings settings = BigtableTableAdminSettings.newBuilder()
                        .setProjectId(options.getBigtableProjectId()).setInstanceId(options.getBigtableInstanceId())
                        .build();
                BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(settings);
                Table table = adminClient.getTable(options.getBigtableTableId());
                columnFamilies = table.getColumnFamilies().stream().map(x -> x.getId()).collect(Collectors.toSet());
            } catch (Exception e) {
                LOG.error("Could not fetch column families: {}", e.getMessage());
            }
            return columnFamilies == null ? new HashSet<String>() : columnFamilies;
        }

        /**
         * Creates a column family in the output Bigtable with the name that is given
         */
        static class CreateColumnFamily extends DoFn<String, String> {

            DataflowPipelineOptions options;

            CreateColumnFamily(DataflowPipelineOptions options) {
                this.options = options;
            }

            @ProcessElement
            public void processElement(@Element String familyName) {
                try {
                    BigtableTableAdminSettings settings = BigtableTableAdminSettings.newBuilder()
                            .setProjectId(options.getBigtableProjectId()).setInstanceId(options.getBigtableInstanceId())
                            .build();
                    BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(settings);
                    Table table = adminClient.getTable(options.getBigtableTableId());
                    try {
                        adminClient.modifyFamilies(ModifyColumnFamiliesRequest.of(table.getId()).addFamily(familyName));
                    } catch (AlreadyExistsException innerException) { // Should not occur, but keeping it here for now
                        LOG.error(innerException.getMessage());
                    }
                } catch (Exception e) {
                    LOG.error("Error while creating column family {} : {}", familyName, e.getMessage());
                }
            }
        }

    }

    /**
     * Constructor
     *
     * @param options are the pipeline options
     */
    public BigtableWriter(DataflowPipelineOptions options) {
        this.options = options;
    }

    /**
     * Function that applies the transform required to create required column
     * families that do not exist
     * 
     * @param rows is the PCollection of rows retrieved after querying BigQuery
     */
    public PCollection<RowData> createNeededColumnFamilies(PCollection<RowData> rows) {
        LOG.info("Applying transform for creating required column families that do not exist!");
        PCollection<RowData> rowsOut = rows.apply(new CheckColumnFamiliesAndReturnInput());
        return rowsOut;
    }

    /**
     * Takes in a PCollection of RowData objects, maps them to Mutation objects and
     * applies them to the output
     * Bigtable table
     * 
     * @param mutations is the PCollection of mutations passed in
     */
    public void applyRowMutations(PCollection<RowData> rows) {
        LOG.info("Applying Bigtable Mutation Transform!");
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
