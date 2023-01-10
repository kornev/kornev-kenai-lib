package kenai.spark.function;

import java.util.Collection;
import java.util.Iterator;

import clojure.lang.IFn;

import scala.Tuple2;

import org.apache.spark.api.java.function.PairFlatMapFunction;


/**
 * Compatibility wrapper for a Spark `PairFlatMapFunction` of one argument
 * which returns a sequence of pairs.
 */
public class PairFlatMapFn1 extends AClojureSerializable implements PairFlatMapFunction<Object, Object, Object> {

    public PairFlatMapFn1(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<Object, Object>> call(Object arg1) throws Exception {
        Collection<Object> result = (Collection<Object>) fn.invoke(arg1);
        Iterator<Object> results = result.iterator();

        return new Iterator<Tuple2<Object, Object>>() {
            public boolean hasNext() {
                return results.hasNext();
            }

            public Tuple2<Object, Object> next() {
                return PairFn1.coercePair(fn, results.next());
            }
        };
    }
}
