package io.trino.plugin.base.cast;

import io.airlift.slice.Slice;
import io.trino.plugin.base.util.DateTimes;
import io.trino.spi.type.LongTimestamp;

import java.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static java.time.ZoneOffset.UTC;

public final class TimestampToVarchar
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private TimestampToVarchar() {}

    public static Slice cast(long precision, long epochMicros)
    {
        return utf8Slice(DateTimes.formatTimestamp((int) precision, epochMicros, 0, UTC, TIMESTAMP_FORMATTER));
    }

    public static Slice cast(long precision, LongTimestamp timestamp)
    {
        return utf8Slice(DateTimes.formatTimestamp((int) precision, timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), UTC, TIMESTAMP_FORMATTER));
    }
}
