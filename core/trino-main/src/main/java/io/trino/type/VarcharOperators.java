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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.plugin.base.cast.FromVarchar;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.function.OperatorType.CAST;

public final class VarcharOperators
{
    private VarcharOperators() {}

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType("varchar(x)") Slice value)
    {
        return FromVarchar.toBoolean(value);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType("varchar(x)") Slice slice)
    {
        return FromVarchar.toDouble(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToFloat(@SqlType("varchar(x)") Slice slice)
    {
        return FromVarchar.toReal(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType("varchar(x)") Slice slice)
    {
        return FromVarchar.toBigint(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType("varchar(x)") Slice slice)
    {
        return FromVarchar.toInteger(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType("varchar(x)") Slice slice)
    {
        return FromVarchar.toSmallint(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType("varchar(x)") Slice slice)
    {
        return FromVarchar.toTinyint(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }
}
