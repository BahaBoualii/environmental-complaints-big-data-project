package hbase.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class InsertJob2ToHbase {
    private Table table;
    private final String tableName = "cdph_complaints";
    private final String columnFamily = "details";

    public void insertDataFromJob2(String inputPath) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            System.out.println("Connecting to HBase...");

            table = connection.getTable(TableName.valueOf(tableName));

            try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.replace("(", "").replace(")", "");
                    String[] parts = line.split(",", 2);
                    if (parts.length != 2) continue;

                    String type = parts[0].trim();
                    String count = parts[1].trim();
                    String rowKey = "type#" + type;

                    Put p = new Put(Bytes.toBytes(rowKey));
                    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(type));
                    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("value"), Bytes.toBytes(count));
                    table.put(p);

                    System.out.println("Inserted row: " + rowKey);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            if (table != null) table.close();
        }
    }

    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: InsertJob2ToHBase <input_path>");
            System.exit(1);
        }
        InsertJob2ToHbase job = new InsertJob2ToHbase();
        job.insertDataFromJob2(args[0]);
    }
}
