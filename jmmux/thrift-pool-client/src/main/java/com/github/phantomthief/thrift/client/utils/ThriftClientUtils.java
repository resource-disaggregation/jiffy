/**
 * 
 */
package com.github.phantomthief.thrift.client.utils;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;

import java.lang.reflect.Method;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <p>
 * ThriftClientUtils class.
 * </p>
 *
 * @author w.vela
 * @version $Id: $Id
 */
public final class ThriftClientUtils {

    private static final Random RANDOM = new Random();
    private static ConcurrentMap<Class<?>, Set<String>> interfaceMethodCache = new ConcurrentHashMap<>();

    private ThriftClientUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * <p>
     * randomNextInt.
     * </p>
     *
     * @return a int.
     */
    public static int randomNextInt() {
        return RANDOM.nextInt();
    }

    /**
     * <p>
     * getInterfaceMethodNames.
     * </p>
     *
     * @param ifaceClass a {@link java.lang.Class} object.
     * @return a {@link java.util.Set} object.
     */
    public static Set<String> getInterfaceMethodNames(Class<?> ifaceClass) {
        return interfaceMethodCache.computeIfAbsent(ifaceClass, i -> of(i.getInterfaces()) //
                .flatMap(c -> of(c.getMethods())) //
                .map(Method::getName) //
                .collect(toSet()));
    }
}
