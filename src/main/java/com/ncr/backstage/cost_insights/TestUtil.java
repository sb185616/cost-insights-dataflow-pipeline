package com.ncr.backstage.cost_insights;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;

import com.google.api.services.bigquery.model.TableRow;

/** Only keeping during testing, will be removed afterwards */
public class TestUtil {

    public static Values<TableRow> getValues(String filepath) throws IOException {
        List<String> strings = Files.readAllLines(Paths.get(filepath));
        List<TableRow> list = stringsToTableRows(strings);
        Values<TableRow> rows = Create.of(list);
        return rows;
    }

    public static List<TableRow> stringsToTableRows(List<String> list) {
        List<TableRow> rows = new ArrayList<TableRow>();
        list.forEach(string -> {
            TableRow row = new TableRow();
            String[] tokens = string.split(",");
            if (tokens.length == 5) {
                row.set("project_name", "null-project");
            }
            for (String token : tokens) {
                String[] kv = token.split("=");
                if (kv[0].strip().equals("cost")) {
                    row.set((String) kv[0], Double.parseDouble(kv[1]));
                    continue;
                }
                row.set(kv[0].strip(), kv[1].strip());
            }
            rows.add(row);
        });
        return rows;
    }

}
