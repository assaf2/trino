package io.trino.plugin.base.cast;

import io.airlift.slice.Slice;
import io.trino.plugin.base.util.DateTimes;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;

import java.time.DateTimeException;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SliceUtf8.trim;
import static io.trino.plugin.base.util.DateTimes.longTimestamp;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.Decimals.rescale;
import static io.trino.spi.type.TimestampType.MAX_PRECISION;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.time.ZoneOffset.UTC;

public final class VarcharToTimestamp
{
    private VarcharToTimestamp() {}

    public static long castToShort(long precision, Slice value)
    {
        try {
            return castToShortTimestamp((int) precision, trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    public static LongTimestamp castToLong(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice value)
    {
        try {
            return castToLongTimestamp((int) precision, trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    public static long castToShortTimestamp(int precision, String value)
    {
        checkArgument(precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision");

        Matcher matcher = DateTimes.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        long epochSecond;
        try {
            epochSecond = ZonedDateTime.of(
                    Integer.parseInt(year),
                    Integer.parseInt(month),
                    Integer.parseInt(day),
                    hour == null ? 0 : Integer.parseInt(hour),
                    minute == null ? 0 : Integer.parseInt(minute),
                    second == null ? 0 : Integer.parseInt(second),
                    0,
                    UTC)
                    .toEpochSecond();
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value, e);
        }

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        // scale to micros
        return epochSecond * MICROSECONDS_PER_SECOND + rescale(fractionValue, actualPrecision, 6);
    }

    public static LongTimestamp castToLongTimestamp(int precision, String value)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_PRECISION, "precision out of range");

        Matcher matcher = DateTimes.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        long epochSecond;
        try {
            epochSecond = ZonedDateTime.of(
                    Integer.parseInt(year),
                    Integer.parseInt(month),
                    Integer.parseInt(day),
                    hour == null ? 0 : Integer.parseInt(hour),
                    minute == null ? 0 : Integer.parseInt(minute),
                    second == null ? 0 : Integer.parseInt(second),
                    0,
                    UTC)
                    .toEpochSecond();
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value, e);
        }

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        return longTimestamp(epochSecond, rescale(fractionValue, actualPrecision, 12));
    }
}
