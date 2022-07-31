/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.scalar.timestamp;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.trino.plugin.base.cast.VarcharToTimestamp;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;

import static io.trino.spi.function.OperatorType.CAST;

@ScalarOperator(CAST)
public final class VarcharToTimestampCast
{
    private VarcharToTimestampCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long castToShort(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice value)
    {
        return VarcharToTimestamp.castToShort(precision, value);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp castToLong(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice value)
    {
        return VarcharToTimestamp.castToLong(precision, value);
    }

    @VisibleForTesting
    public static long castToShortTimestamp(int precision, String value)
    {
        return VarcharToTimestamp.castToShortTimestamp(precision, value);
    }

    @VisibleForTesting
    public static LongTimestamp castToLongTimestamp(int precision, String value)
    {
        return VarcharToTimestamp.castToLongTimestamp(precision, value);
    }
}
