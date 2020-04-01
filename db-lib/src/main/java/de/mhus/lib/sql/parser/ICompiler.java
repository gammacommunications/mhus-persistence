/**
 * Copyright 2018 Mike Hummel
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.sql.parser;

import java.util.Calendar;
import java.util.Date;

import de.mhus.lib.core.MCast;
import de.mhus.lib.core.parser.ParsingPart;

public interface ICompiler {

    boolean isParseAttributes();

    ParsingPart compileFunction(FunctionPart function);

    default String toSqlDateValue(Object string) {
        return "'" + MCast.toString(string) + "'";
    }

    default String valueToString(Object value) {
        return MCast.objectToString(value);
    }

    default String valueToNumber(Object value) {
        if (value == null) return "0";
        if (value instanceof Date) {
            return String.valueOf(((Date) value).getTime());
        }
        if (value instanceof Calendar) {
            return String.valueOf(((Calendar) value).getTimeInMillis());
        }
        if (value instanceof Number) {
            return String.valueOf(((Number) value).longValue());
        }
        return MCast.objectToString(value);
    }

    default String valueToFloating(Object value) {
        if (value == null) return "0";
        if (value instanceof Date) {
            return String.valueOf(((Date) value).getTime());
        }
        if (value instanceof Calendar) {
            return String.valueOf(((Calendar) value).getTimeInMillis());
        }
        if (value instanceof Number) {
            return String.valueOf(((Number) value).doubleValue());
        }
        return MCast.objectToString(value);
    }

    String escape(String text);

    String toBoolValue(boolean value);
}
