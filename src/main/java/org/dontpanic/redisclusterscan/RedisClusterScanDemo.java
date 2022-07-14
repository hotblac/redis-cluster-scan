package org.dontpanic.redisclusterscan;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.primes.Primes;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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
    private final ExecutorService scanExecutorService = Executors.newFixedThreadPool(JEDIS_CLUSTER_NODES.size());

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        RedisClusterScanDemo demo = new RedisClusterScanDemo();
        demo.runDemo();
    }

    public void runDemo() throws ExecutionException, InterruptedException {
        try (JedisCluster cluster = new JedisCluster(JEDIS_CLUSTER_NODES)) {
            initData(cluster);
            //scanBroken(cluster); // Fails with: Cluster mode only supports SCAN command with MATCH pattern containing hash-tag ( curly-brackets enclosed string )

            // Example 1: Determine frequency of first digit of all keys
            long[] results = scanAllNodes(cluster, this::firstDigitCount, new long[10], this::zipSum);
            System.out.println(Arrays.toString(results));

            // Example 2: Find values associated with all keys that are a prime number
            List<String> primeKeys = scanAllNodes(cluster, this::primeKeys, new ArrayList<>(), this::joinList);
            System.out.println(String.join(",", primeKeys));
        } finally {
            scanExecutorService.shutdown();
        }
    }

    private void initData(JedisCluster cluster) {
        StopWatch timer = StopWatch.createStarted();
        System.out.println("Adding " + NUM_KEYS + " keys...");
        LongStream.range(0, NUM_KEYS).parallel().forEach(i -> cluster.set(KEY_PREFIX + i, randomAlphanumeric(12)));
        timer.stop();
        System.out.println("Added " + NUM_KEYS + " keys in " + timer.formatTime());
    }

    private void scanBroken(JedisCluster cluster) {
        ScanParams scanParams = new ScanParams().count(SCAN_BATCH);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> scanResult = cluster.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            System.out.println("First key in batch: "  + keys.get(0));
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }

    private <T> T scanAllNodes(JedisCluster cluster, Function<List<String>, T> keyFunction, T identity, BinaryOperator<T> accumulator) throws ExecutionException, InterruptedException {
        StopWatch timer = StopWatch.createStarted();
        T accumulatedResult = identity;
        List<Future<T>> results = new ArrayList<>();

        // Scan all nodes in parallel
        for (ConnectionPool node : cluster.getClusterNodes().values()) {
            try (Jedis j = new Jedis(node.getResource())) {
                Future<T> result = scanExecutorService.submit(() -> scan(j, keyFunction, identity, accumulator));
                results.add(result);
            }
        }

        // Await and accumulate all scan results
        for (Future<T> result: results) {
            accumulatedResult = accumulator.apply(accumulatedResult, result.get());
        }

        timer.stop();
        System.out.println("Scanned " + NUM_KEYS + " keys in " + timer.formatTime());

        return accumulatedResult;
    }

    /**
     * Scan keys on a single Redis node and return accumulated result of a function on the keys
     * @param node Jedis
     * @param keyFunction function to be performed on keys
     * @param identity identity for accumulator
     * @param accumulator combine two keyFunction results into an accumulated result
     * @return results accumulated over complete scan of redis keys
     * @param <T> return type
     */
    private <T> T scan(Jedis node, Function<List<String>, T> keyFunction, T identity, BinaryOperator<T> accumulator) {
        T accumulatedResult = identity;
        ScanParams scanParams = new ScanParams().count(SCAN_BATCH);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> scanResult = node.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            T thisResult = keyFunction.apply(keys);
            accumulatedResult = accumulator.apply(accumulatedResult, thisResult);
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return accumulatedResult;
    }

    /**
     * Zip two arrays with sum function.
     * zipSum([1, 2, 3], [5, 5, 5]) -> [6, 7, 8]
     */
    private long[] zipSum(long[] result1, long[] result2) {
        return IntStream.range(0, 10)
                .mapToLong(i -> result1[i] + result2[i])
                .toArray();
    }

    private <T> List<T> joinList(List<T> l1, List<T> l2) {
        return Stream.of(l1, l2).flatMap(List::stream).collect(Collectors.toList());
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

    private List<String> primeKeys(List<String> keys) {
        return keys.stream().filter(this::isPrimeKey).collect(Collectors.toList());
    }

    private boolean isPrimeKey(String key) {
        return Primes.isPrime(Integer.parseInt(StringUtils.removeStart(key, KEY_PREFIX)));
    }
}
