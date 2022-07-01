package com.ncr.backstage.cost_insights;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import com.google.api.services.bigquery.model.TableReference;
import com.google.bigtable.repackaged.com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;

/**
 * Class that runs the pipeline.
 * Reads billing info from given BigQuery tables, aggregates the costs by day,
 * and exports the transformed data to the given Bigtable table
 */
public class PipelineRunner {

    /**
     * Reads in a JSON file and returns a list of TableReference objects that point
     * to the input BigQuery tables
     * 
     * @param filepath is the path for the JSON file
     * @return list of table references
     */
    private static List<TableReference> getTableReferences(String filepath) throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(filepath));
        List<TableReference> references = new Gson().fromJson(reader, new TypeToken<List<TableReference>>() {
        }.getType());
        reader.close();
        return references;
    }

    /**
     * Reads in a JSON file and returns a string pointing to the Bigtable table to
     * which the aggregated costs are to be written
     * 
     * @param filepath is the path for the JSON file
     * @return string reference to bigtable
     */
    private static String getOutputBigtable(String filepath) throws IOException {
        return null;
    }

    public static void main(final String[] args) throws Exception {

        List<TableReference> inputs = PipelineRunner.getTableReferences(args[1]);
        String output = PipelineRunner.getOutputBigtable(args[2]);

        ZonedDateTime aggregationDate = LocalDate.now().atStartOfDay(ZoneOffset.UTC).minusDays(2);
        String dateString = aggregationDate.toLocalDate().toString();
        Long timestamp = aggregationDate.toEpochSecond();

        Pipeline pipeline = Pipeline.create();
    }
}
