package org.dontpanic.redisclusterscan;

import org.apache.commons.lang3.time.StopWatch;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Set;

public class RedisClusterScanDemo {

    private static final long NUM_KEYS = 1_000_000;
    private static final int SCAN_BATCH = 1000;
    private static final Set<HostAndPort> JEDIS_CLUSTER_NODES = Set.of(
            new HostAndPort("127.0.0.1", 6371),
            new HostAndPort("127.0.0.1", 6372),
            new HostAndPort("127.0.0.1", 6373)
    );

    public static void main(String[] args) {
        RedisClusterScanDemo demo = new RedisClusterScanDemo();
        demo.runDemo();
    }

    public void runDemo() {
        try (JedisCluster cluster = new JedisCluster(JEDIS_CLUSTER_NODES)) {
            initData(cluster);
            scanAllNodes(cluster);
        }
    }

    private void initData(JedisCluster cluster) {
        StopWatch timer = StopWatch.createStarted();
        System.out.println("Adding " + NUM_KEYS + " keys...");
        for (int i=0; i<NUM_KEYS; i++) {
            cluster.set("key:" + i, "value:" + i);
        }
        timer.stop();
        System.out.println("Added " + NUM_KEYS + " keys in " + timer.formatTime());
    }

    private void scanBroken(JedisCluster cluster) {
        ScanParams scanParams = new ScanParams().count(SCAN_BATCH);
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanResult<String> scanResult = cluster.scan(cursor, scanParams);
        while(!scanResult.isCompleteIteration()) {
            List<String> keys = scanResult.getResult();
            System.out.println("First key in batch: "  + keys.get(0));
        }
    }

    private void scanAllNodes(JedisCluster cluster) {
        int keysScanned = 0;
        for (ConnectionPool node : cluster.getClusterNodes().values()) {
            try (Jedis j = new Jedis(node.getResource())) {
                int nodeKeys = scanNode(j);
                keysScanned += nodeKeys;
            }
            System.out.println("Keys scanned: " + keysScanned);
        }

    }

    private int scanNode(Jedis node) {
        int keysScanned = 0;
        ScanParams scanParams = new ScanParams().count(SCAN_BATCH);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> scanResult = node.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            keysScanned += keys.size();
            System.out.println("Scanned " + keys.size());
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return keysScanned;
    }
}
