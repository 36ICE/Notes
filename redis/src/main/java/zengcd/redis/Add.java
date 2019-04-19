package zengcd.redis;

import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.*;

/**
 * @author ZCD 2019/4/19
 * @since 1.0.0
 */
public class Add {

    private static final Integer Num = 100000;

    private static final String key_1 = "set_1";

    private static final String host = "localhost";

    private static final Integer batchSize = 10000;

    /**
     * 逐条处理  本机测试 1.2万/s
     */
    public static void add1() {
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = jedisPool.getResource();
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        long s = startTime;
        try {
            System.out.println("start add1 test...");
            for (int i = 1; i <= Num; i++) {
                jedis.sadd(key_1, DigestUtils.sha1Hex(i + ""));
                if (i % batchSize == 0) {
                    endTime = System.currentTimeMillis();
                    printAddRate(startTime, endTime);
                    startTime = System.currentTimeMillis();
                }
            }
        } finally {
            printTime(s, System.currentTimeMillis());
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * pipeline批量处理  本机测试 50万/s
     */
    public static void add2() {
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = jedisPool.getResource();
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        long s = startTime;
        try {
            Pipeline pipeline = jedis.pipelined();
            pipeline.multi();
            Transaction transaction = jedis.multi();
            System.out.println("start add2 test...");
            for (int i = 1; i <= Num; i++) {
                pipeline.sadd(key_1, DigestUtils.sha1Hex(i + ""));
                if (i % batchSize == 0) {
                    pipeline.exec();
                    pipeline.multi();
                    endTime = System.currentTimeMillis();
                    printAddRate(startTime, endTime);
                    startTime = System.currentTimeMillis();
                }
            }
            pipeline.exec();
            pipeline.multi();
        } finally {

            printTime(s, System.currentTimeMillis());
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * transaction事务批量处理  本机测试 34万/s
     */
    public static void add3() {
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = jedisPool.getResource();
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        long s = startTime;
        try {
            Transaction transaction = jedis.multi();
            System.out.println("start add3 test...");
            for (int i = 1; i <= Num; i++) {
                transaction.sadd(key_1, DigestUtils.sha1Hex(i + ""));
                if (i % batchSize == 0) {
                    transaction.exec();
                    transaction = jedis.multi();
                    endTime = System.currentTimeMillis();
                    printAddRate(startTime, endTime);
                    startTime = System.currentTimeMillis();
                }
            }
            transaction.exec();
        } finally {
            printTime(s, System.currentTimeMillis());
            jedisPool.returnResource(jedis);
        }
    }

    public static void main(String[] args) {
        add1();
        add2();
        add3();
    }

    public static void printAddRate(Long startTime, Long endTime) {
        System.out.format("%.4f 万条/s\n", batchSize / 10000.0 * 1000.0 / (endTime - startTime + 1));
    }

    public static void printTime(Long startTime, Long endTime) {
        System.out.format("%d totle time : %.4f s , %.4f 万条/s\n", Num, (endTime - startTime + 1) / 1000.0, Num / 10000.0 * 1000.0 / (endTime - startTime + 1));
    }
}
