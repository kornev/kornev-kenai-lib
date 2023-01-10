package kenai.spark.function;

import java.util.Collection;

import clojure.lang.IFn;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentVector;

import scala.Tuple2;

import org.apache.spark.api.java.function.PairFunction;


/**
 * Compatibility wrapper for a Spark `PairFunction` of one argument which
 * returns a pair.
 */
public class PairFn1 extends AClojureSerializable implements PairFunction<Object, Object, Object> {

    public PairFn1(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    public Tuple2<Object, Object> call(Object arg1) throws Exception {
        return coercePair(fn, fn.invoke(arg1));
    }

    /**
     * Coerce a result value into a Scala `Tuple2` as the result of a function.
     *
     * @param fn the function which produced the result, to report in error messages
     * @param result object to try to coerce
     * @return a Scala tuple with two values
     */
    @SuppressWarnings("unchecked")
    public static Tuple2<Object, Object> coercePair(IFn fn, Object result) {
        // Null can't be coerced.
        if (result == null) {
            throw new RuntimeException("Wrapped pair function " + fn + " returned a null");
        // Scala tuples can be returned directly.
        } else if (result instanceof Tuple2) {
            return (Tuple2<Object, Object>) result;
        // Use key/value from Clojure map entries to construct a tuple.
        } else if (result instanceof IMapEntry) {
            IMapEntry entry = (IMapEntry) result;
            return new Tuple2<>(entry.key(), entry.val());
        // Try to generically coerce a sequential result into a tuple.
        } else if (result instanceof IPersistentVector) {
            IPersistentVector vector = (IPersistentVector) result;
            if (vector.count() != 2) {
                throw new RuntimeException("Wrapped pair function " + fn + " returned a vector without exactly two values: " + vector.count());
            }
            return new Tuple2<>(vector.nth(0), vector.nth(1));
        // Unknown type, can't coerce.
        } else {
            throw new RuntimeException("Wrapped pair function " + fn + " returned an invalid pair type: " + result.getClass().getName());
        }
    }
}
