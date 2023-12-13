package org.example;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;

public class BigtableDemo {

    public static void main(String[] args) throws IOException {

        BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                .setProjectId("spanner-demo-326919")
                .setInstanceId("bigtable-otel-instance1")
                .build();

        BigtableDataSettings.enableBuiltinMetrics();

        StackdriverStatsExporter.createAndRegister(
                StackdriverStatsConfiguration.builder()
                        .setProjectId("spanner-demo-326919")
                        .build()
        );

        BigtableDataSettings.enableOpenCensusStats();
        // Enable GFE metric views
        BigtableDataSettings.enableGfeOpenCensusStats();


        // setup table and insertion logic
        String tableId = "table1";
        BigtableDataClient dataClient = BigtableDataClient.create(settings);
        String columnFamily = "cf1";
        String columnQualifier = "qualifier1";
        String value = "3";

        // Insert "value" into 10 rows
        for (int i = 1; i <= 10; i++) {
            String rowKey = "row" + i;

            // Create a Mutation object to represent the data to be inserted
            Mutation mutation = Mutation.create().setCell(columnFamily, columnQualifier, String.valueOf(ByteString.copyFromUtf8(value)));

            // Create a RowMutation object and apply the mutation to the row
            RowMutation rowMutation = RowMutation.create(tableId, rowKey)
                    .setCell(columnFamily, columnQualifier, String.valueOf(ByteString.copyFromUtf8(value)));

            // Apply the RowMutation to the table
            dataClient.mutateRow(rowMutation);

            //success for row
            System.out.println("Data added to Bigtable for row: " + rowKey);
        }
        //success for operation
        System.out.println("Data added to rows in Bigtable successfully.");

    }
}

//use to create a table & family via code
//        BigtableTableAdminClient tableAdminClient = BigtableTableAdminClient
//                .create(projectId, instanceId);
//        try {
//            tableAdminClient.createTable(
//                    CreateTableRequest.of("my-table")
//                            .addFamily("my-family")
//            );
//        } finally {
//            tableAdminClient.close();
//        }
