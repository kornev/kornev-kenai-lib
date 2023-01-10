package kenai.spark.function;

import java.util.Collection;

import clojure.lang.IFn;

import org.apache.spark.api.java.function.Function2;


/**
 * Compatibility wrapper for a Spark `Function2` of two arguments.
 */
public class Fn2 extends AClojureSerializable implements Function2<Object, Object, Object> {

    public Fn2(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    public Object call(Object arg1, Object arg2) throws Exception {
        return fn.invoke(arg1, arg2);
    }
}
