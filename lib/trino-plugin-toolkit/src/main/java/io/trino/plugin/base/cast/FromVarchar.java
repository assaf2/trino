package io.trino.plugin.base.cast;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.Int128;

import java.math.BigDecimal;

import static io.airlift.slice.SliceUtf8.trim;
import static io.trino.plugin.base.util.DateTimeUtils.parseDate;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.Decimals.overflows;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class FromVarchar
{
    private FromVarchar() {}

    public static long toInteger(Slice slice)
    {
        try {
            return Integer.parseInt(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INT", slice.toStringUtf8()));
        }
    }

    public static long toBigint(Slice slice)
    {
        try {
            return Long.parseLong(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", slice.toStringUtf8()));
        }
    }

    public static long toReal(Slice slice)
    {
        try {
            return Float.floatToIntBits(Float.parseFloat(slice.toStringUtf8()));
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to REAL", slice.toStringUtf8()));
        }
    }

    public static double toDouble(Slice slice)
    {
        try {
            return Double.parseDouble(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DOUBLE", slice.toStringUtf8()));
        }
    }

    public static boolean toBoolean(Slice value)
    {
        if (value.length() == 1) {
            byte character = toUpperCase(value.getByte(0));
            if (character == 'T' || character == '1') {
                return true;
            }
            if (character == 'F' || character == '0') {
                return false;
            }
        }
        if ((value.length() == 4) &&
                (toUpperCase(value.getByte(0)) == 'T') &&
                (toUpperCase(value.getByte(1)) == 'R') &&
                (toUpperCase(value.getByte(2)) == 'U') &&
                (toUpperCase(value.getByte(3)) == 'E')) {
            return true;
        }
        if ((value.length() == 5) &&
                (toUpperCase(value.getByte(0)) == 'F') &&
                (toUpperCase(value.getByte(1)) == 'A') &&
                (toUpperCase(value.getByte(2)) == 'L') &&
                (toUpperCase(value.getByte(3)) == 'S') &&
                (toUpperCase(value.getByte(4)) == 'E')) {
            return false;
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BOOLEAN", value.toStringUtf8()));
    }

    private static byte toUpperCase(byte b)
    {
        return isLowerCase(b) ? ((byte) (b - 32)) : b;
    }

    private static boolean isLowerCase(byte b)
    {
        return (b >= 'a') && (b <= 'z');
    }

    public static long toShortDecimal(Slice value, long precision, long scale, long tenToScale)
    {
        BigDecimal result;
        String stringValue = value.toString(UTF_8);
        try {
            result = new BigDecimal(stringValue).setScale(DecimalConversions.intScale(scale), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value is not a number.", stringValue, precision, scale));
        }

        if (overflows(result, precision)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value too large.", stringValue, precision, scale));
        }

        return result.unscaledValue().longValue();
    }

    public static Int128 toLongDecimal(Slice value, long precision, long scale, Int128 tenToScale)
    {
        BigDecimal result;
        String stringValue = value.toString(UTF_8);
        try {
            result = new BigDecimal(stringValue).setScale(DecimalConversions.intScale(scale), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value is not a number.", stringValue, precision, scale));
        }

        if (overflows(result, precision)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value too large.", stringValue, precision, scale));
        }

        return Int128.valueOf(result.unscaledValue());
    }

    public static long toDate(Slice value)
    {
        try {
            return parseDate(trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to date: " + value.toStringUtf8(), e);
        }
    }

    public static long toTinyint(Slice slice)
    {
        try {
            return Byte.parseByte(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", slice.toStringUtf8()));
        }
    }

    public static long toSmallint(Slice slice)
    {
        try {
            return Short.parseShort(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", slice.toStringUtf8()));
        }
    }
}
