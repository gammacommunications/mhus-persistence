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

import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;

import de.mhus.lib.core.MCast;
import de.mhus.lib.core.MDate;
import de.mhus.lib.core.MSql;
import de.mhus.lib.core.parser.CompiledString;
import de.mhus.lib.core.parser.ParseException;
import de.mhus.lib.core.parser.ParseReader;
import de.mhus.lib.core.parser.Parser;
import de.mhus.lib.core.parser.ParsingPart;
import de.mhus.lib.core.parser.StringPart;

/**
 * SqlCompiler class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class SqlCompiler implements Parser, ICompiler {

    //	private static Log log = Log.getLog(SqlCompiler.class);

    private ICompiler compiler = null;

    /** Constructor for SqlCompiler. */
    public SqlCompiler() {
        compiler = this;
    }

    /**
     * Constructor for SqlCompiler.
     *
     * @param compiler a {@link de.mhus.lib.sql.parser.ICompiler} object.
     */
    public SqlCompiler(ICompiler compiler) {
        this.compiler = compiler;
    }

    /** {@inheritDoc} */
    @Override
    public CompiledString compileString(String in) throws ParseException {

        MainPart root = new MainPart(compiler);

        StringReader sr = new StringReader(in);

        ParseReader pr;
        try {
            pr = new ParseReader(sr);
        } catch (IOException e) {
            throw new ParseException(e);
        }
        root.parse(pr);

        return new CompiledString(new StringPart[] {root});
    }

    /** {@inheritDoc} */
    @Override
    public boolean isParseAttributes() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public ParsingPart compileFunction(FunctionPart function) {
        return function;
    }

    /**
     * Setter for the field <code>compiler</code>.
     *
     * @param compiler a {@link de.mhus.lib.sql.parser.ICompiler} object.
     */
    public void setCompiler(ICompiler compiler) {
        this.compiler = compiler;
    }

    /**
     * Getter for the field <code>compiler</code>.
     *
     * @return a {@link de.mhus.lib.sql.parser.ICompiler} object.
     */
    public ICompiler getCompiler() {
        return compiler;
    }

    /** {@inheritDoc} */
    @Override
    public String toSqlDateValue(Object value) {
        if (value == null) return "null";
        if (value instanceof Calendar) return toSqlDate(((Calendar) value).getTime());
        if (value instanceof Date) return toSqlDate((Date) value);
        if (value instanceof Number) {
            Date date = new Date(((Number) value).longValue());
            return toSqlDate(date);
        }
        Date date = MCast.toDate(value, null);
        if (date == null) return "null";
        return toSqlDate(date);
    }

    public String toSqlDate(Date date) {
        return "'" + MDate.toIsoDate(date) + "'";
    }

    /** {@inheritDoc} */
    @Override
    public String escape(String text) {
        return MSql.escape(text);
    }

    @Override
    public String toBoolValue(boolean value) {
        return value ? "1" : "0";
    }
}
