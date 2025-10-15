// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.exceptions;

import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/*
The 3 methods below are taken from Apache Commons
Lang3 ExceptionUtils class, licensed under Apache License 2.0
 */
public class ExceptionUtils {
    private static final int NOT_FOUND = -1;
    static final String WRAPPED_MARKER = " [wrapped] ";

    public static String @NotNull [] getRootCauseStackTrace(final Throwable throwable) {
        return getRootCauseStackTraceList(throwable).toArray(new String[0]);
    }

    public static @NotNull List<String> getRootCauseStackTraceList(final Throwable throwable) {
        if (throwable == null) {
            return Collections.emptyList();
        }
        final Throwable[] throwables = getThrowables(throwable);
        final int count = throwables.length;
        final List<String> frames = new ArrayList<>();
        List<String> nextTrace = getStackFrameList(throwables[count - 1]);
        for (int i = count; --i >= 0;) {
            final List<String> trace = nextTrace;
            if (i != 0) {
                nextTrace = getStackFrameList(throwables[i - 1]);
                removeCommonFrames(trace, nextTrace);
            }
            if (i == count - 1) {
                frames.add(throwables[i].toString());
            } else {
                frames.add(WRAPPED_MARKER + throwables[i].toString());
            }
            frames.addAll(trace);
        }
        return frames;
    }

    static @NotNull List<String> getStackFrameList(final Throwable throwable) {
        final String stackTrace = getStackTrace(throwable);
        final String linebreak = System.lineSeparator();
        final StringTokenizer frames = new StringTokenizer(stackTrace, linebreak);
        final List<String> list = new ArrayList<>();
        boolean traceStarted = false;
        while (frames.hasMoreTokens()) {
            final String token = frames.nextToken();
            // Determine if the line starts with "<whitespace>at"
            final int at = token.indexOf("at");
            if (at != NOT_FOUND && token.substring(0, at).trim().isEmpty()) {
                traceStarted = true;
                list.add(token);
            } else if (traceStarted) {
                break;
            }
        }
        return list;
    }

    private static String getStackTrace(final Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        final StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw, true));
        return sw.toString();
    }

    private static Throwable @NotNull [] getThrowables(final Throwable throwable) {
        return getThrowableList(throwable).toArray(new Throwable[0]);
    }

    private static @NotNull List<Throwable> getThrowableList(Throwable throwable) {
        final List<Throwable> list = new ArrayList<>();
        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = throwable.getCause();
        }
        return list;
    }

    public static void removeCommonFrames(final @NotNull List<String> causeFrames, final @NotNull List<String> wrapperFrames) {
        int causeFrameIndex = causeFrames.size() - 1;
        int wrapperFrameIndex = wrapperFrames.size() - 1;
        while (causeFrameIndex >= 0 && wrapperFrameIndex >= 0) {
            final String causeFrame = causeFrames.get(causeFrameIndex);
            final String wrapperFrame = wrapperFrames.get(wrapperFrameIndex);
            if (causeFrame.equals(wrapperFrame)) {
                causeFrames.remove(causeFrameIndex);
            }
            causeFrameIndex--;
            wrapperFrameIndex--;
        }
    }
}
