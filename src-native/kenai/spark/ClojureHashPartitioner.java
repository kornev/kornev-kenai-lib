package kenai.spark;

import static clojure.lang.Util.hasheq;

import org.apache.spark.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kenai.spark.function.Fn1;


/**
 * A Partitioner Similar to Spark's HashPartitioner, which also accepts a key
 * function to translate an Object into a hashable key, and uses Clojure's
 * hash function instead of Object.hashCode().
 */
public class ClojureHashPartitioner extends Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(ClojureHashPartitioner.class);

    private final int numPartitions;
    private final Fn1 keyFn;

    public ClojureHashPartitioner(int numPartitions, Fn1 keyFn) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive, got " + numPartitions);
        }
        if (keyFn == null) {
            throw new IllegalArgumentException("Key function must not be null");
        }
        this.numPartitions = numPartitions;
        this.keyFn = keyFn;
    }

    public int numPartitions() {
        return this.numPartitions;
    }

    public int getPartition(Object key) {
        Object transformedKey = null;
        try {
            transformedKey = this.keyFn.call(key);
        } catch (Exception e) {
            logger.error("Key function threw an exception, so this key will be hashed as if it were null."
                         + " This is likely to cause skewed partitioning.", e);
        }
        return Math.floorMod(hasheq(transformedKey), this.numPartitions);
    }
}
