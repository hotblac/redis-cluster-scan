package org.dontpanic.redisclusterscan;

import org.apache.commons.lang3.time.StopWatch;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Set;

public class RedisClusterScanDemo {

    private static final long NUM_KEYS = 1_000_000;
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
}
