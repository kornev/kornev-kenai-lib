package kenai.spark.function;

import java.util.Collection;

import clojure.lang.IFn;

import org.apache.spark.api.java.function.VoidFunction;


/**
 * Compatibility wrapper for a Spark `VoidFunction` of one argument.
 */
public class VoidFn1 extends AClojureSerializable implements VoidFunction<Object> {

    public VoidFn1(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    public void call(Object arg1) throws Exception {
        fn.invoke(arg1);
    }
}
