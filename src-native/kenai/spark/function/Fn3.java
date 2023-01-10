package kenai.spark.function;

import java.util.Collection;

import clojure.lang.IFn;

import org.apache.spark.api.java.function.Function3;


/**
 * Compatibility wrapper for a Spark `Function3` of three arguments.
 */
public class Fn3 extends AClojureSerializable implements Function3<Object, Object, Object, Object> {

    public Fn3(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    public Object call(Object arg1, Object arg2, Object arg3) throws Exception {
        return fn.invoke(arg1, arg2, arg3);
    }
}
