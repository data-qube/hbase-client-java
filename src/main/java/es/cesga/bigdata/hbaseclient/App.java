package es.cesga.bigdata.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        // Option 1 (recommended): Place hbase-site.xml on src/main/resources
        // Option 2: Configure the hbase connection properties here:
        // - Zookeeper addresses
//        conf.set("hbase.zookeeper.quorum", "192.168.130.141,192.168.130.142,192.168.130.143");
        conf.set("hbase.zookeeper.quorum","192.168.58.155,192.168.58.156,192.168.58.157");
        // - Non-default znode parent (e.g. HDP with security disabled)
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // Create a connection (you can use a try with resources block)
        conf.setInt("hbase.rpc.timeout", 20000);
        conf.setInt("hbase.client.operation.timeout", 30000);
        conf.setInt("hbase.client.scaner.timeout.period", 200000);
        Connection connection = ConnectionFactory.createConnection(conf);

        // Creating a table with a random name
        Admin admin = connection.getAdmin();
        UUID uuid = UUID.randomUUID();
//        TableName tableName = TableName.valueOf(uuid.toString());
        TableName tableName = TableName.valueOf("buffered-mutator-test");
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor cf = new HColumnDescriptor(Bytes.toBytes("cf"));
        tableDescriptor.addFamily(cf);
        logger.info("Creating table {}", tableName);
        admin.createTable(tableDescriptor);

        // Using the table
        Table table = connection.getTable(tableName);

        logger.info("Adding a row");
        Put put = new Put(Bytes.toBytes("0001"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("Javier"));
        table.put(put);

        logger.info("Reading a row");
        Get get = new Get(Bytes.toBytes("0001"));
        Result r = table.get(get);
        byte[] value = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        logger.info("Result: Row: {} Value: {}", r, Bytes.toString(value));

        logger.info("Full scan");
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            logger.info("Row: {}", res);
        }
        scanner.close();

        logger.info("Restricted Scan");
        Scan scan2 = new Scan();
        scan2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"))
                .setStartRow(Bytes.toBytes("0003"))
                .setStopRow(Bytes.toBytes("0005"));
        ResultScanner scanner2 = table.getScanner(scan2);
        for (Result res2 : scanner2) {
            logger.info(res2 + "");
        }
        scanner2.close();

        // Close the table
        table.close();

        // Delete the table
        logger.info("Disabling table {}", tableName);
        admin.disableTable(tableName);
        logger.info("Deleting table {}", tableName);
        admin.deleteTable(tableName);

        // Close the connection
        connection.close();

        System.exit(0);
    }
}
