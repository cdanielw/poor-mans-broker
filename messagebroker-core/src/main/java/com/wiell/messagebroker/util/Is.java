package com.wiell.messagebroker.util;

public final class Is {
    public static void notNull(Object o, String message) {
        if (o == null)
            throw new IllegalArgumentException(message);
    }

    public static void haveText(String s, String message) {
        if (s == null || s.trim().isEmpty())
            throw new IllegalArgumentException(message + ". Value: '" + s + "'");
    }

    public static void greaterThenZero(int i, String message) {
        if (i < 0)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }

    public static void zeroOrGreater(int i, String message) {
        if (i <= 0)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }
}
