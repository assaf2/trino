package io.trino.plugin.base.util;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.toIntExact;

public final class DateTimeUtils
{
    private DateTimeUtils() {}

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    public static int parseDate(String value)
    {
        // in order to follow the standard, we should validate the value:
        // - the required format is 'YYYY-MM-DD'
        // - all components should be unsigned numbers
        // https://github.com/trinodb/trino/issues/10677
        return toIntExact(TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value)));
    }

    public static String printDate(int days)
    {
        return DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(days));
    }
}
