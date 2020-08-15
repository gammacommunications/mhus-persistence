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
