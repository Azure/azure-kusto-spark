package com.microsoft.kusto.spark.utils;

import java.time.Duration;

public class DurationFormatter {
    /**
     * Formats a Duration as "dd:HH:mm:ss" (zero-padded).
     */
    public static String format(Duration duration) {
        long totalSeconds = duration.getSeconds();
        long days = totalSeconds / (24 * 3600);
        long hours = (totalSeconds % (24 * 3600)) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        return String.format("%02d:%02d:%02d:%02d", days, hours, minutes, seconds);
    }
}

