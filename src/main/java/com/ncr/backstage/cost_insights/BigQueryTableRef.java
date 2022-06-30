package com.ncr.backstage.cost_insights;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.List;

import com.google.bigtable.repackaged.com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Class to represent the information required to refer to a specific BigQuery
 * Table
 */
public class BigQueryTableRef {

    public String getProject() {
        return this.project;
    }

    public String getDataset() {
        return this.dataset;
    }

    public String getTable() {
        return this.table;
    }

    /** GCP Project */
    private final String project;
    /** BigQuery dataset within that project */
    private final String dataset;
    /** BigQuery tables within the given dataset */
    private final String table;

    /**
     * Takes in the file path for the JSON file containing table info, and returns a
     * list of BigQueryTableOptions objects
     * 
     * @param filepath is the path to the file that contains the information for the
     *                 tables
     * @return returns a list of BigQueryTableOptions objects
     */
    public static List<BigQueryTableRef> getTableOptions(final String filepath) throws IOException {
        final Reader reader = Files.newBufferedReader(Paths.get(filepath));
        final List<BigQueryTableRef> tables = new Gson().fromJson(reader,
                new TypeToken<List<BigQueryTableRef>>() {
                }.getType());
        reader.close();
        return tables;
    }

    /**
     * Constructor
     * 
     * @param project is the GCP Project
     * @param dataset is the dataset to select within the project
     * @param table   is the specific table to be used within the given dataset
     */
    public BigQueryTableRef(final String project, final String dataset, final String table) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
    }

    @Override
    public String toString() {
        return String.format("%s:%s.%s", this.project, this.dataset, this.table);
    }

    public static void main(final String[] args) throws Exception {
        // BigQueryTableRef.getTableOptions("/Users/SB185616/code/dataflow_pipeline/tableInfo/tableInfo.json");
        // System.out.println(LocalDate.now().atStartOfDay(ZoneOffset.UTC).minusDays(2).toEpochSecond());
        // Arrays.asList(BigQueryColumns.class.getEnumConstants()).forEach(System.out::println);
    }
}
