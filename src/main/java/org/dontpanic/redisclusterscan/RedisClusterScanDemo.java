package org.dontpanic.redisclusterscan;

import org.apache.commons.lang3.time.StopWatch;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public class RedisClusterScanDemo {

    private static final long NUM_KEYS = 1_000_000;
    private static final int SCAN_BATCH = 1000;
    private static final String KEY_PREFIX = "key:";
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
            //initData(cluster);
            scanAllNodes(cluster);
        }
    }

    private void initData(JedisCluster cluster) {
        StopWatch timer = StopWatch.createStarted();
        System.out.println("Adding " + NUM_KEYS + " keys...");
        for (int i=0; i<NUM_KEYS; i++) {
            cluster.set(KEY_PREFIX + i, randomAlphanumeric(12));
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
        List<long[]> results = new ArrayList<>();
        long keysScanned = 0;
        for (ConnectionPool node : cluster.getClusterNodes().values()) {
            try (Jedis j = new Jedis(node.getResource())) {
                long[] result = scan(j, this::firstDigitCount, this::zip);
                results.add(result);
                keysScanned += Arrays.stream(result).sum();
            }
            System.out.println("Keys scanned: " + keysScanned);
        }

        long[] finalResult = zip(results);
        System.out.println(Arrays.toString(finalResult));
    }

    private <T, R> R scan(Jedis node, Function<List<String>, T> keyFunction, Function<List<T>, R> mergeFunction) {
        List<T> results = new ArrayList<>();
        ScanParams scanParams = new ScanParams().count(SCAN_BATCH);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> scanResult = node.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            results.add(keyFunction.apply(keys));
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return mergeFunction.apply(results);
    }

    /**
     * Merge a List of count arrays to a single array
     */
    private long[] zip(List<long[]> results) {
        return IntStream.range(0, 10)
                .mapToLong(i -> sum(results, i))
                .toArray();
    }

    /**
     * Sum all values at index i of given arrays
     */
    private long sum(List<long[]> results, int i) {
        return results.stream().mapToLong(result -> result[i]).sum();
    }

    /**
     * Count of first digits of given keys.
     * @param keys prefixed by {@link #KEY_PREFIX}
     * @return Array of size 10 (0-9) where each array index contains the count of keys beginning with that digit.
     */
    private long[] firstDigitCount(List<String> keys) {
        final int firstDigitIndex = KEY_PREFIX.length();
        Map<Character, Long> characterCountMap = keys.stream()
                .collect(Collectors.groupingBy(k -> k.charAt(firstDigitIndex), Collectors.counting()));
        return IntStream.range(0, 10)
                .mapToLong(i -> characterCountMap.getOrDefault(Character.forDigit(i, 10), 0L))
                .toArray();
    }
}
