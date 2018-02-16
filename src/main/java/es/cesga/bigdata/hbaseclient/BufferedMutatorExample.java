package es.cesga.bigdata.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class BufferedMutatorExample {

    private static final Logger logger = LoggerFactory.getLogger(BufferedMutatorExample.class);
    private static final int THREADS = 4;
    private static final int TASKS = 20;

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        // Option 1 (recommended): Place hbase-site.xml on src/main/resources
        // Option 2: Configure the hbase connection properties here:
        // - Zookeeper addresses
        conf.set("hbase.zookeeper.quorum", "10.112.13.19,10.112.13.18,10.112.13.17");
        // - Non-default znode parent (e.g. HDP with security disabled)
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                logger.info("Failed to send put: " + e.getRow(i));
            }
        };

        TableName tableName = TableName.valueOf("buffered-mutator-test");
        BufferedMutatorParams params = new BufferedMutatorParams(tableName).listener(listener);

        try (
                Connection conn = ConnectionFactory.createConnection(conf);
                BufferedMutator mutator = conn.getBufferedMutator(params)
        ) {
            ExecutorService workerPool = Executors.newFixedThreadPool(THREADS);
            List<Future<Void>> futures = new ArrayList<>(TASKS);

            for (int i = 0; i < TASKS; i++) {
                futures.add(workerPool.submit(() -> {
                    logger.info("Worker launched");
                    UUID uuid = UUID.randomUUID();
                    Put p = new Put(Bytes.toBytes(uuid.toString()));
                    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("Javier"));
                    mutator.mutate(p);
                    // You can call mutator.flush() to force sending any pending puts of this worker
                    return null;
                }));
            }

            logger.info("Waiting for all tasks to complete");
            for (Future<Void> f : futures) {
                f.get(2, TimeUnit.MINUTES);
            }
            logger.info("All tasks completed");
            workerPool.shutdown();
        } catch (IOException e) {
            logger.info("Exception while creating or freeing resources", e);
        }
    }
}
