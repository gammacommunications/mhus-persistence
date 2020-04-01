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

import de.mhus.lib.core.MSql;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.parser.ParsingPart;
import de.mhus.lib.core.parser.StringCompiler;
import de.mhus.lib.core.parser.StringPart;

public class SimpleQueryCompiler extends StringCompiler implements ICompiler {

    //	private static Log log = Log.getLog(SimpleQueryCompiler.class);

    @Override
    protected StringPart createDefaultAttributePart(String part) {
        ParameterPart out = new ParameterPart(this);
        out.attribute = MString.split(part, ",");
        return out;
    }

    @Override
    public boolean isParseAttributes() {
        return true;
    }

    @Override
    public ParsingPart compileFunction(FunctionPart function) {
        return function;
    }

    @Override
    public String escape(String text) {
        return MSql.escape(text);
    }

    @Override
    public String toBoolValue(boolean value) {
        return value ? "1" : "0";
    }
}
