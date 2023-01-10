package kenai.spark.function;

import java.util.Collection;
import java.util.Comparator;

import clojure.lang.IFn;


/**
 * Compatibility wrapper for a `Comparator` of two arguments.
 */
public class ComparatorFn2 extends AClojureSerializable implements Comparator<Object> {

    public ComparatorFn2(IFn fn, Collection<String> namespaces) {
        super(fn, namespaces);
    }

    public int compare(Object arg1, Object arg2) {
        return (int) fn.invoke(arg1, arg2);
    }
}
