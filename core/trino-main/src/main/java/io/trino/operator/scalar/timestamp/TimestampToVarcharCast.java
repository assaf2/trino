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

import io.airlift.slice.Slice;
import io.trino.plugin.base.cast.TimestampToVarchar;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;

import static io.trino.spi.function.OperatorType.CAST;

@ScalarOperator(CAST)
public final class TimestampToVarcharCast
{
    private TimestampToVarcharCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") long epochMicros)
    {
        return TimestampToVarchar.cast(precision, epochMicros);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return TimestampToVarchar.cast(precision, timestamp);
    }
}
