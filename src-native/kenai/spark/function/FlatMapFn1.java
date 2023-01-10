package kenai.spark.function;

import java.util.Iterator;
import java.util.Collection;

import clojure.lang.IFn;

import org.apache.spark.api.java.function.FlatMapFunction;


/**
 * Compatibility wrapper for a Spark `FlatMapFunction` of one argument.
 */
public class FlatMapFn1 extends AClojureSerializable implements FlatMapFunction<Object, Object> {

    public FlatMapFn1(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    @SuppressWarnings("unchecked")
    public Iterator<Object> call(Object arg1) throws Exception {
        Collection<Object> result = (Collection<Object>) fn.invoke(arg1);
        return result.iterator();
    }
}
