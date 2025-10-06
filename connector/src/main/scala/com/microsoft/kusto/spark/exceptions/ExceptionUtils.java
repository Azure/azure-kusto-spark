package com.microsoft.kusto.spark.exceptions;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ExceptionUtils {
    public static String getRootCauseStackTrace(Throwable throwable) {
        Throwable rootCause = getRootCause(throwable);
        return rootCause.getMessage();
    }

    public static @NotNull String getStackTrace(Throwable throwable) {
        Objects.requireNonNull(throwable);
        StringBuilder sb = new StringBuilder();
        sb.append(throwable.toString()).append("\n");
        for (StackTraceElement element : throwable.getStackTrace()) {
            sb.append("\tat ").append(element.toString()).append("\n");
        }
        return sb.toString();
    }

    public static Throwable getRootCause(Throwable throwable) {
        Objects.requireNonNull(throwable);
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }
}
