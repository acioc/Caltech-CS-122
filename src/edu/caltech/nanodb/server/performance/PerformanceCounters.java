package edu.caltech.nanodb.server.performance;



import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;


/**
 * This class provides a basic performance-counter mechanism that can be used
 * concurrently from many different threads.  It allows us to record
 * performance statistics from the database, and ultimately to expose them
 * through SQL queries as well.
 *
 * @review (Donnie) I really don't like that this is a static class, because
 *         it prevents us from having multiple sets of performance counters.
 *         This is just the easiest way to add counters to NanoDB in the short
 *         run.  In the future, make this non-static, and maybe stick an
 *         instance of it on the NanoDB Server object.  We can put it into
 *         thread-local storage to make it easy to access.
 */
public class PerformanceCounters {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(PerformanceCounters.class);


    private static ConcurrentHashMap<String, AtomicInteger> counters =
        new ConcurrentHashMap<String, AtomicInteger>();


    private static AtomicInteger getCounter(String counterName) {
        // Do this in two steps so that we can try to avoid allocating an
        // AtomicInteger unless it looks like we need to.
        AtomicInteger counter = counters.get(counterName);
        if (counter == null)
            counter = counters.putIfAbsent(counterName, new AtomicInteger());

        return counter;
    }


    public static int inc(String counterName) {
        return getCounter(counterName).incrementAndGet();
    }


    public static int add(String counterName, int value) {
        return getCounter(counterName).addAndGet(value);
    }


    public static int dec(String counterName) {
        return getCounter(counterName).decrementAndGet();
    }


    public static int sub(String counterName, int value) {
        return add(counterName, -value);
    }


    public static int get(String counterName) {
        return getCounter(counterName).get();
    }


    public static int clear(String counterName) {
        return getCounter(counterName).getAndSet(0);
    }


    public static void clearAll() {
        counters.clear();
    }


    public static Set<String> getCounterNames() {
        return new HashSet<String>(counters.keySet());
    }
}
