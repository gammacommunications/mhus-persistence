package de.mhus.lib.test.adb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mhus.lib.adb.DbCollection;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.QueryParser;
import de.mhus.lib.adb.util.ParserJdbcDebug;
import de.mhus.lib.core.MApi;
import de.mhus.lib.core.logging.Log.LEVEL;
import de.mhus.lib.test.adb.model.Person;
import de.mhus.lib.tests.TestCase;

public class ParserTest extends TestCase {

    @BeforeAll
    public static void begin() {
        try {
            MApi.get().getLogFactory().setDefaultLevel(LEVEL.TRACE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSelectAll() throws Throwable {
        System.out.println(">>> testSelectAll");
        ParserJdbcDebug parser = new ParserJdbcDebug();
        parser.init(null);
        QueryParser.parse("select * from book", parser);

        System.out.println("Table: " + parser.getEntityName());
        System.out.println("Columns: " + parser.getColumnNames());
        System.out.println("Query: " + parser.getQualification());

        assertEquals("book", parser.getEntityName());
        assertEquals("", parser.getQualification());
        assertEquals("[*]", parser.getColumnNames().toString());
    }

    @Test
    public void testSelect() throws Throwable {
        System.out.println(">>> testSelect");

        ParserJdbcDebug parser = new ParserJdbcDebug();
        parser.init(null);
        QueryParser.parse("select * from Book where name='test' and created < '1.1.2020'", parser);

        System.out.println("Table: " + parser.getEntityName());
        System.out.println("Columns: " + parser.getColumnNames());
        System.out.println("Query: " + parser.getQualification());

        assertEquals("book", parser.getEntityName());
        assertEquals(
                "$db.book.name$ = 'test' AND $db.book.created$ < '1.1.2020'",
                parser.getQualification());
        assertEquals("[*]", parser.getColumnNames().toString());
    }

    @Test
    public void testSelectColumns() throws Throwable {
        System.out.println(">>> testSelectColumns");

        ParserJdbcDebug parser = new ParserJdbcDebug();
        parser.init(null);
        QueryParser.parse("select name,created from book where name='test'", parser);

        System.out.println("Table: " + parser.getEntityName());
        System.out.println("Columns: " + parser.getColumnNames());
        System.out.println("Query: " + parser.getQualification());

        assertEquals("book", parser.getEntityName());
        assertEquals("$db.book.name$ = 'test'", parser.getQualification());
        assertEquals("[name, created]", parser.getColumnNames().toString());
    }

    @Test
    public void testRealQuery() throws Throwable {
        DbManager manager = AdbTest.createBookstoreManager();

        // create persons
        Person p = new Person();
        p.setName("Klaus Mustermann");
        manager.createObject(p);
        @SuppressWarnings("unused")
        UUID p1 = p.getId();

        p.setId(null);
        p.setName("Alex Admin");
        manager.createObject(p);
        UUID p2 = p.getId();

        DbCollection<Person> res =
                QueryParser.getByQualification(
                        manager, "select * from Person where name='Alex Admin'", null);
        assertTrue(res.hasNext());

        Person current = res.next();
        assertEquals(p2, current.getId());

        assertFalse(res.hasNext());

        manager.getPool().close();
    }
}
