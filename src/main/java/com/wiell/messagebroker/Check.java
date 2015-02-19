package com.wiell.messagebroker;

final class Check {
    static void notNull(Object o, String message) {
        if (o == null)
            throw new IllegalArgumentException(message);
    }

    static void haveText(String s, String message) {
        if (s == null || s.trim().isEmpty())
            throw new IllegalArgumentException(message + ". Value: '" + s + "'");
    }

    static void greaterThenZero(int i, String message) {
        if (i < 0)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }

    static void zeroOrGreater(int i, String message) {
        if (i <= 0)
            throw new IllegalArgumentException(message + ". Value: '" + i + "'");
    }
}
