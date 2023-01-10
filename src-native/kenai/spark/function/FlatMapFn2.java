package kenai.spark.function;

import java.util.Iterator;
import java.util.Collection;

import clojure.lang.IFn;

import org.apache.spark.api.java.function.FlatMapFunction2;


/**
 * Compatibility wrapper for a Spark `FlatMapFunction2` of two arguments.
 */
public class FlatMapFn2 extends AClojureSerializable implements FlatMapFunction2<Object, Object, Object> {

    public FlatMapFn2(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    @SuppressWarnings("unchecked")
    public Iterator<Object> call(Object arg1, Object arg2) throws Exception {
        Collection<Object> result = (Collection<Object>) fn.invoke(arg1, arg2);
        return result.iterator();
    }
}
