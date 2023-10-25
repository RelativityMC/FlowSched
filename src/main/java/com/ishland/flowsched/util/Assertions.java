package com.ishland.flowsched.util;

public class Assertions {

    public static void assertTrue(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    public static void assertTrue(boolean state, String format, Object... args) {
        if (!state) {
            throw new AssertionError(String.format(format, args));
        }
    }

    public static void assertTrue(boolean value) {
        if (!value) {
            throw new AssertionError();
        }
    }



}
