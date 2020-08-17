/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.adb;

import java.util.List;
import java.util.Map;

import de.mhus.lib.core.parser.ParseException;
import de.mhus.lib.errors.MException;
import de.mhus.lib.xdb.XdbService;
import de.mhus.lib.xdb.XdbType;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;

public interface QueryParser extends StatementVisitor {

    public static QueryParser parse(XdbService manager, String sql) throws ParseException {
        QueryParser parser = manager.createParser();
        parse(sql, parser);
        return parser;
    }

    public static void parse(String sql, QueryParser parser) throws ParseException {
        try {
            Select stmt = (Select) CCJSqlParserUtil.parse(sql);
            stmt.accept(parser);
        } catch (Exception t) {
            throw new ParseException(sql, t);
        }
    }

    public String getEntityName();

    public List<String> getColumnNames();

    public String getQualification();

    public static <T> DbCollection<T> getByQualification(
            XdbService manager, String sql, Map<String, Object> parameterValues) throws MException {
        QueryParser parser = parse(manager, sql);
        XdbType<T> type = manager.getType(parser.getEntityName());
        return type.getByQualification(parser.getQualification(), parameterValues);
    }
}
