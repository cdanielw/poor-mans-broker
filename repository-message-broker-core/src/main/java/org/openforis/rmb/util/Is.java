package org.openforis.rmb.util;

import java.util.Collection;
import java.util.Map;

public final class Is {
    private Is() { }

    public static void notNull(Object o, String message) {
        if (o == null)
            throw new IllegalArgumentException(message);
    }

    public static void hasText(String s, String message) {
        if (s == null || s.trim().isEmpty())
            throw new IllegalArgumentException(message + ". Value: '" + s + "'");
    }

    public static void greaterThenZero(int i, String message) {
        if (i <= 0)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }

    public static void greaterThenZero(long l, String message) {
        if (l <= 0)
            throw new IllegalArgumentException(message + ". Value: '" + l + "'");
    }

    public static void zeroOrGreater(int i, String message) {
        if (i < 0)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }

    public static void lessThanEqual(int i, int lessThanValue, String message) {
        if (i > lessThanValue)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }

    public static void notEmpty(Collection<?> collection, String message) {
        if (collection == null || collection.isEmpty())
            throw new IllegalArgumentException(message + ".");
    }

    public static void notEmpty(Map<?, ?> map, String message) {
        if (map == null || map.isEmpty())
            throw new IllegalArgumentException(message + ".");
    }
}
