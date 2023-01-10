package kenai.spark.function;

import java.util.Collection;

import clojure.lang.IFn;

import org.apache.spark.api.java.function.Function;


/**
 * Compatibility wrapper for a Spark `Function` of one argument.
 */
public class Fn1 extends AClojureSerializable implements Function<Object, Object> {

    public Fn1(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    public Object call(Object arg1) throws Exception {
        return fn.invoke(arg1);
    }
}
