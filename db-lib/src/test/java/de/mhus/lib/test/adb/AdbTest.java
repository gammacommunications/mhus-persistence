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
package de.mhus.lib.test.adb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mhus.lib.adb.DbCollection;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbManagerJdbc;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.adb.query.Db;
import de.mhus.lib.core.MApi;
import de.mhus.lib.core.MStopWatch;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.config.NodeConfig;
import de.mhus.lib.core.logging.Log.LEVEL;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DbPoolBundle;
import de.mhus.lib.test.adb.model.Book;
import de.mhus.lib.test.adb.model.BookStoreSchema;
import de.mhus.lib.test.adb.model.Finances;
import de.mhus.lib.test.adb.model.Person;
import de.mhus.lib.test.adb.model.Person2;
import de.mhus.lib.test.adb.model.Regal;
import de.mhus.lib.test.adb.model.Store;

public class AdbTest {

    @BeforeAll
    public static void begin() {
        try {
            MApi.get().getLogFactory().setDefaultLevel(LEVEL.TRACE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int poolCnt = 0;

    public DbPoolBundle createPool(String name) throws Exception {

        name = name.toLowerCase().trim() + (poolCnt++);

        String jdbcDriver = "";
        String jdbcAdminUrl = "";
        String jdbcAdminUser = "";
        String jdbcAdminPass = "";

        String jdbcUrl = "";
        String jdbcUser = "";
        String jdbcPass = "";

        String databaseName = "";

        jdbcDriver = System.getProperty("jdbcDriver", "");
        jdbcAdminUrl = System.getProperty("jdbcAdminUrl", "");
        jdbcAdminUser = System.getProperty("jdbcAdminUser", "");
        jdbcAdminPass = System.getProperty("jdbcAdminPass", "");

        jdbcUrl = System.getProperty("jdbcUrl", "");
        jdbcUser = System.getProperty("jdbcUser", "");
        jdbcPass = System.getProperty("jdbcPass", "");

        // postgresql
        //		jdbcDriver = "org.postgresql.Driver";
        //		jdbcAdminUrl = "jdbc:postgresql://10.10.11.58:32768/";
        //		jdbcAdminUser = "postgres";
        //		jdbcAdminPass = "nein";

        // mysql
        //		jdbcDriver = "com.mysql.jdbc.Driver";
        //		jdbcAdminUrl = "jdbc:mysql://10.10.11.58:32769/";
        //		jdbcAdminUser = "root";
        //		jdbcAdminPass = "nein";

        // hsqldb - fallback
        if (MString.isEmpty(jdbcDriver)) {
            jdbcDriver = "org.hsqldb.jdbcDriver";
            jdbcUrl = "jdbc:hsqldb:mem:" + name;
            jdbcUser = "sa";
            jdbcPass = "";
        }

        // prepare databases
        if (MString.isSet(jdbcAdminUrl)) {
            System.out.println(">>> Recreate Database");
            if (MString.isEmpty(jdbcUser)) jdbcUser = "usr_mhus" + name;
            if (MString.isEmpty(jdbcPass)) jdbcPass = "x" + (int) (Math.random() * 10000d);
            if (MString.isEmpty(databaseName)) databaseName = "mhus" + name;
            if (MString.isEmpty(jdbcUrl)) jdbcUrl = jdbcAdminUrl + databaseName;

            Class.forName(jdbcDriver);
            Connection con =
                    DriverManager.getConnection(jdbcAdminUrl, jdbcAdminUser, jdbcAdminPass);

            if (jdbcDriver.contains("mysql")) {

                try {
                    con.prepareStatement("DROP DATABASE " + databaseName).execute();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
                try {
                    con.prepareStatement("DROP USER " + jdbcUser).execute();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }

                con.prepareStatement("CREATE DATABASE " + databaseName).execute();
                con.prepareStatement("CREATE USER " + jdbcUser).execute();

                con.prepareStatement(
                                "GRANT ALL PRIVILEGES ON "
                                        + databaseName
                                        + ".* TO "
                                        + jdbcUser
                                        + ";")
                        .execute();

                con.prepareStatement(
                                "SET PASSWORD FOR '" + jdbcUser + "'=PASSWORD('" + jdbcPass + "')")
                        .execute();

            } else if (jdbcDriver.contains("postgresql")) {

                try {
                    con.prepareStatement("DROP DATABASE " + databaseName).execute();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
                try {
                    con.prepareStatement("DROP USER " + jdbcUser).execute();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }

                con.prepareStatement("CREATE DATABASE " + databaseName).execute();
                con.prepareStatement("CREATE USER " + jdbcUser).execute();

                con.prepareStatement(
                                "GRANT ALL PRIVILEGES ON DATABASE "
                                        + databaseName
                                        + " TO "
                                        + jdbcUser
                                        + ";")
                        .execute();

                con.prepareStatement("ALTER USER " + jdbcUser + " PASSWORD '" + jdbcPass + "'")
                        .execute();
            }

            con.close();
            System.out.println("<<<");
        }

        NodeConfig cdb = new NodeConfig();
        NodeConfig cconfig = new NodeConfig();

        cdb.setProperty("driver", jdbcDriver);
        cdb.setProperty("url", jdbcUrl);
        cdb.setProperty("user", jdbcUser);
        cdb.setProperty("pass", jdbcPass);

        cconfig.setConfig("test", cdb);

        DbPoolBundle pool = new DbPoolBundle(cconfig, null);

        //			System.out.println(">>> Cleanup Database");
        //			DbConnection con = pool.getPool("test").getConnection();
        //			con.createStatement("DROP TABLE book_").execute(null);
        //			con.createStatement("DROP TABLE bookstoreschema_").execute(null);
        //			con.createStatement("DROP TABLE finances_").execute(null);
        //			con.createStatement("DROP TABLE person2_").execute(null);
        //			con.createStatement("DROP TABLE person_").execute(null);
        //			con.createStatement("DROP TABLE regal_").execute(null);
        //			con.createStatement("DROP TABLE store_").execute(null);
        //			con.commit();
        //
        //			con.close();
        //
        //			System.out.println("<<<");

        return pool;
    }

    @Test
    public void testModel() throws Throwable {

        DbPool pool = createPool("testModel").getPool("test");

        BookStoreSchema schema = new BookStoreSchema();

        MStopWatch timer = new MStopWatch();
        timer.start();

        //		MApi.get().getLogFactory().setDefaultLevel(LEVEL.TRACE);
        DbManager manager = new DbManagerJdbc("", pool, schema);
        //		MApi.get().getLogFactory().setDefaultLevel(LEVEL.INFO);

        // create persons
        Person p = new Person();
        p.setName("Klaus Mustermann");
        manager.createObject(p);
        UUID p1 = p.getId();

        p.setId(null);
        p.setName("Alex Admin");
        manager.createObject(p);
        UUID p2 = p.getId();

        {
            List<Person> list =
                    manager.getByQualification(Db.query(Person.class).asc("name"))
                            .toCacheAndClose();
            assertEquals(3, list.size());
            assertEquals("Alex Admin", list.get(0).getName());
            assertEquals("Hausmeister Krause", list.get(1).getName());
            assertEquals("Klaus Mustermann", list.get(2).getName());
        }
        // create books

        Book b = new Book();
        b.setName("Yust Another Boring Book");
        manager.createObject(b);
        UUID b1 = b.getId();

        b.setId(null);
        b.setName("Mystic Almanach");
        manager.createObject(b);
        UUID b2 = b.getId();

        // get a book and modify

        b = manager.getObject(Book.class, b1);
        b.setLendToId(p1);
        manager.saveObject(b);

        b.setLendToId(null);
        manager.saveObject(b);

        b.setLendToId(p2);
        b.setAuthorId(new UUID[] {p1});
        manager.saveObject(b);

        Book copy = manager.getObject(Book.class, b.getId());
        assertEquals(copy.getAuthorId()[0], p1);

        // test relations
        assertNotNull(b.getLendTo());
        Person rel = b.getLendTo().getRelation();
        assertNotNull(rel);
        assertEquals(rel.getId(), p2);

        assertNotNull(rel.getLendTo());
        List<Book> retRel = rel.getLendTo().getRelations();
        assertNotNull(retRel);
        assertEquals(1, retRel.size());
        assertEquals(b.getId(), retRel.get(0).getId());

        retRel.add(manager.getObject(Book.class, b2));

        manager.saveObject(rel);

        rel.getLendTo().reset();
        retRel = rel.getLendTo().getRelations();
        assertNotNull(retRel);
        assertEquals(2, retRel.size());

        b = manager.getObject(Book.class, b1);
        assertNotNull(b.getLendToId());
        b.getLendTo().setRelation(null);
        manager.saveObject(b);
        assertNull(b.getLendToId());
        b.getLendTo().setRelation(p);
        manager.saveObject(b);
        assertNotNull(b.getLendToId());

        // test getFields ...
        List<String> names = manager.getAttributeByQualification("name", Db.query(Book.class));
        System.out.println(names);
        assertEquals(2, names.size());

        // remove book

        manager.deleteObject(b);

        b = manager.getObject(Book.class, b1);
        assertNull(b);

        // test selection

        System.out.println("----------------------");
        DbCollection<Person> col =
                manager.executeQuery(new Person(), "select * from $db.person$", null);
        int count = 0;
        for (Person pp : col) {
            System.out.println("--- " + pp.getId() + " " + pp.getName());
            count++;
        }
        assertEquals(count, 3);

        System.out.println("----------------------");

        col = manager.getByQualification(new Person(), null, null);
        count = 0;
        for (Person pp : col) {
            System.out.println("--- " + pp.getId() + " " + pp.getName());
            count++;
        }
        assertEquals(count, 3);

        System.out.println("----------------------");

        col = manager.getByQualification(new Person(), "$db.person.name$ like 'Klaus%'", null);
        count = 0;
        for (Person pp : col) {
            System.out.println("--- " + pp.getId() + " " + pp.getName());
            count++;
        }
        assertEquals(count, 1);

        System.out.println("----------------------");

        //    	col = manager.getByQualification(new Person(), Db.query( Db.like(Db.db(Person.class,
        // "name"), Db.value("'Klaus%'")) ) );
        col =
                manager.getByQualification(
                        Db.query(Person.class).like(Db.attr("name"), Db.fix("'Klaus%'")));
        count = 0;
        for (Person pp : col) {
            System.out.println("--- " + pp.getId() + " " + pp.getName());
            count++;
        }
        assertEquals(count, 1);

        System.out.println("----------------------");

        // test a native sql execute - remove all persons

        DbConnection con = manager.getPool().getConnection();
        con.createStatement("DELETE FROM $db.person$", null).execute(manager.getNameMapping());
        con.commit();

        System.out.println("----------------------");
        col = manager.executeQuery(new Person(), "select * from $db.person$", null);
        count = 0;
        for (Person pp : col) {
            System.out.println("--- " + pp.getId() + " " + pp.getName());
            count++;
        }
        assertEquals(count, 0);

        // -------------
        // test comfortable object

        Store s1 = new Store();
        s1.setName("Creasy Bookstore");
        s1.setAddress("The Oaks\nDublin");
        s1.create(manager);
        s1.setSqlDate(new java.sql.Date(0));
        s1.setAddress("The Lakes\nDublin");
        s1.save();

        // test change in another session and reload
        Store s2 = manager.getObject(Store.class, s1.getId());
        s2.setAddress("");
        s2.save();

        s1.reload();
        assertEquals(s1.getAddress(), s2.getAddress());
        assertEquals(s1.getSqlDate().toString(), new java.sql.Date(0).toString());

        // remove and check behavior of updates
        s1.delete();

        try {
            s2.reload();
            assertTrue(false);
        } catch (MException e) {
            System.out.println(e);
        }

        try {
            s2.save();
            assertTrue(false);
        } catch (MException e) {
            System.out.println(e);
        }

        // -------------
        // test access control

        Finances f = new Finances();
        f.setActiva(10);
        f.setPassiva(10);
        f.setStore(s1.getId());
        f.create(manager);

        f.setActiva(20);
        f.save();

        f.setNewConfidential("write");
        f.save();

        try {
            f.save();
            assertTrue(false);
        } catch (MException e) {
            System.out.println(e);
        }

        f.reload();

        try {
            f.save();
            assertTrue(false);
        } catch (MException e) {
            System.out.println(e);
        }

        f.setConfidential("read"); // hack :)
        f.setNewConfidential("read");
        f.save();

        try {
            f.reload();
            assertTrue(false);
        } catch (MException e) {
            System.out.println(e);
        }

        f.setConfidential("read"); // hack :)
        f.setNewConfidential("remove");
        f.save();
        f.reload();

        try {
            f.delete();
            assertTrue(false);
        } catch (MException e) {
            System.out.println(e);
        }

        f.setConfidential("read"); // hack :)
        f.setNewConfidential("");
        f.save();
        f.reload();
        f.delete();

        // -------------
        // test dynamic objects
        {
            Regal r = new Regal();
            r.setValue("store", s1.getId());
            r.setValue("name", "regal 1");
            r.create(manager);

            r.setValue("name", "regal 22113221");
            r.save();

            Regal r2 = manager.getObject(Regal.class, r.getValue("id"));
            assertNotNull(r2);

            r2.reload();

            r2.delete();
        }

        timer.stop();
        System.out.println("Time: " + timer.getCurrentTimeAsString());

        pool.close();
    }

    @Test
    public void testDbQuery() throws Exception {

        DbPool pool = createPool("testDbQuery").getPool("test");

        BookStoreSchema schema = new BookStoreSchema();

        MStopWatch timer = new MStopWatch();
        timer.start();

        DbManager manager = new DbManagerJdbc("", pool, schema);

        Store store1 = manager.inject(new Store());
        store1.save();

        {
            List<Store> res =
                    manager.getByQualification(Db.query(Store.class).isNull("name"))
                            .toCacheAndClose();
            assertEquals(1, res.size());
        }
        {
            List<Store> res =
                    manager.getByQualification(Db.query(Store.class).isNotNull("name"))
                            .toCacheAndClose();
            assertEquals(0, res.size());
        }

        // Test of sub queries - first create a scenario

        Person2 p1 = manager.inject(new Person2());
        p1.setName("Max");
        p1.save();

        Person2 p2 = manager.inject(new Person2());
        p2.setName("Moritz");
        p2.save();

        Person2 p3 = manager.inject(new Person2());
        p3.setName("Witwe Bolte");
        p3.save();

        store1.setName("NYC");
        store1.setPrincipal(p1.getId());
        store1.save();

        Store store2 = manager.inject(new Store());
        store2.setName("LA");
        store2.setPrincipal(p2.getId());
        store2.save();

        Store store3 = manager.inject(new Store());
        store3.setName("LA West");
        store3.setPrincipal(p3.getId());
        store3.save();

        {
            AQuery<Person2> q1 = Db.query(Person2.class).eq("name", "Max");
            List<Person2> res = manager.getByQualification(q1).toCacheAndClose();
            assertEquals(1, res.size());
            assertEquals("Max", res.get(0).getName());
        }

        //		{
        //			LambdaUtil.debugOut = true;
        //			AQuery<Person2> q1 = Db.query(Person2.class).eq(M.n(Person2::getName), "Max");
        //			List<Person2> res = manager.getByQualification(q1).toCacheAndClose();
        //			assertEquals(1, res.size());
        //			assertEquals("Max",res.get(0).getName());
        //		}

        {
            AQuery<Person2> q1 = Db.query(Person2.class).eq("name", "Max");
            AQuery<Store> q2 = Db.query(Store.class).in("principal", "id", q1);
            List<Store> res = manager.getByQualification(q2).toCacheAndClose();
            assertEquals(1, res.size());
            assertEquals("NYC", res.get(0).getName());
        }

        {
            AQuery<Person2> q1 = Db.query(Person2.class).eq("name", "Moritz");
            AQuery<Store> q2 = Db.query(Store.class).in("principal", "id", q1);
            List<Store> res = manager.getByQualification(q2).toCacheAndClose();
            assertEquals(1, res.size());
            assertEquals("LA", res.get(0).getName());
        }

        // test limit

        {
            AQuery<Store> q = Db.query(Store.class).limit(1);
            List<Store> res = manager.getByQualification(q).toCacheAndClose();
            assertEquals(1, res.size());
        }

        {
            AQuery<Store> q = Db.query(Store.class).like("name", "LA%").limit(1);
            List<Store> res = manager.getByQualification(q).toCacheAndClose();
            assertEquals(1, res.size());
        }

        pool.close();
    }

    @Test
    public void testReconnect() throws Exception {
        DbPool pool = createPool("testReconnect").getPool("test");

        //		MApi.get().getLogFactory().setDefaultLevel(LEVEL.TRACE);
        BookStoreSchema schema1 = new BookStoreSchema();
        DbManager manager1 = new DbManagerJdbc("", pool, schema1);

        BookStoreSchema schema2 = new BookStoreSchema();
        DbManager manager2 = new DbManagerJdbc("", pool, schema2);

        manager1.reconnect();
        manager2.reconnect();

        pool.close();
    }

    @Test
    public void testDataTypes() throws Exception {
        DbPool pool = createPool("testDataTypes").getPool("test");

        BookStoreSchema schema = new BookStoreSchema();
        DbManager manager = new DbManagerJdbc("", pool, schema);

        Store store = manager.inject(new Store());

        store.setIntValue(Integer.MAX_VALUE);
        store.setByteValue(Byte.MAX_VALUE);
        store.setShortValue(Short.MAX_VALUE);
        store.setCharValue(' ');
        store.setBigDecimalValue(BigDecimal.TEN);
        store.setDoubleValue(Double.MAX_VALUE);
        store.setFloatValue(Float.MAX_VALUE);
        store.getBlobValue().put("a", "b");
        store.setSqlDate(new Date(1000));

        store.save();
        UUID id = store.getId();

        store = null;

        store = manager.getObject(Store.class, id);
        assertEquals(Integer.MAX_VALUE, store.getIntValue());
        assertEquals(Byte.MAX_VALUE, store.getByteValue());
        assertEquals(Short.MAX_VALUE, store.getShortValue());
        assertEquals(' ', store.getCharValue());
        assertEquals(BigDecimal.TEN, store.getBigDecimalValue());
        assertEquals(Double.MAX_VALUE, store.getDoubleValue());
        assertEquals(Float.MAX_VALUE, store.getFloatValue());
        assertEquals("b", store.getBlobValue().get("a"));
        assertEquals(1000, store.getSqlDate().getTime());
    }

    @Test
    public void testDateType() throws Exception {
        DbPool pool = createPool("testDataTypes").getPool("test");

        BookStoreSchema schema = new BookStoreSchema();
        DbManager manager = new DbManagerJdbc("", pool, schema);

        for (int i = 1; i < 10; i++) {
            Store store = manager.inject(new Store());
            store.setName("Store " + i);
            store.setIntValue(i);
            store.setSqlDate(new Date(i * 1000 * 60 * 60 * 24));
            store.save();
        }

        {
            System.out.println(">>> test query with date");
            List<Store> res =
                    manager.getByQualification(
                                    Db.query(Store.class)
                                            .ge("sqldate", new Date(3 * 1000 * 60 * 60 * 24))
                                            .le("sqldate", new Date(5 * 1000 * 60 * 60 * 24)))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println(o.getName() + " " + o.getSqlDate()));
            assertEquals(3, res.size());
        }
        {
            System.out.println(">>> test query with number");
            List<Store> res =
                    manager.getByQualification(
                                    Db.query(Store.class)
                                            .ge("sqldate", 3 * 1000 * 60 * 60 * 24)
                                            .le("sqldate", 5 * 1000 * 60 * 60 * 24))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println(o.getName() + " " + o.getSqlDate()));
            assertEquals(3, res.size());
        }
        {
            System.out.println(">>> test query with calendar");
            Calendar from = Calendar.getInstance(Locale.UK);
            from.setTime(new Date(3 * 1000 * 60 * 60 * 24));
            Calendar to = Calendar.getInstance(Locale.UK);
            to.setTime(new Date(5 * 1000 * 60 * 60 * 24));
            List<Store> res =
                    manager.getByQualification(
                                    Db.query(Store.class).ge("sqldate", from).le("sqldate", to))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println(o.getName() + " " + o.getSqlDate()));
            assertEquals(3, res.size());
        }
    }

    @Test
    public void testTextType() throws Exception {
        DbPool pool = createPool("testTextType").getPool("test");

        BookStoreSchema schema = new BookStoreSchema();
        DbManager manager = new DbManagerJdbc("", pool, schema);

        //	      // print all
        //        {
        //            System.out.println(">>> ALL");
        //            List<Store> res = manager.getAll(Store.class).toCacheAndClose();
        //            res.forEach(o -> System.out.println("--- All: " + o.getName()));
        //        }

        for (int i = 1; i < 10; i++) {
            Store store = manager.inject(new Store());
            store.setName("Store " + i);
            store.setIntValue(i);
            store.setSqlDate(new Date(i * 1000 * 60 * 60 * 24));
            store.save();
        }

        for (int i = 1; i < 10; i++) {
            Store store = manager.inject(new Store());
            store.setName("" + i);
            store.setIntValue(i);
            store.save();
        }

        {
            System.out.println(">>> test query with text");
            List<Store> res =
                    manager.getByQualification(Db.query(Store.class).eq("name", "5"))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println("--- Found: " + o.getName()));
            assertEquals(1, res.size());
        }
        {
            System.out.println(">>> test query with number");
            List<Store> res =
                    manager.getByQualification(Db.query(Store.class).eq("name", 5))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println("--- Found: " + o.getName()));
            assertEquals(1, res.size());
        }
    }

    @Test
    public void testNumberType() throws Exception {
        DbPool pool = createPool("testNumberType").getPool("test");

        BookStoreSchema schema = new BookStoreSchema();
        DbManager manager = new DbManagerJdbc("", pool, schema);

        for (int i = 1; i < 10; i++) {
            Store store = manager.inject(new Store());
            store.setName("" + i);
            store.setIntValue(i * 1000 * 60 * 60 * 24);
            store.save();
        }

        {
            System.out.println(">>> test query with text");
            List<Store> res =
                    manager.getByQualification(
                                    Db.query(Store.class)
                                            .eq(
                                                    "intvalue",
                                                    String.valueOf(5 * 1000 * 60 * 60 * 24)))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println(o.getName() + " " + o.getIntValue()));
            assertEquals(1, res.size());
        }
        {
            System.out.println(">>> test query with number");
            List<Store> res =
                    manager.getByQualification(
                                    Db.query(Store.class).eq("intvalue", 5 * 1000 * 60 * 60 * 24))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println(o.getName() + " " + o.getIntValue()));
            assertEquals(1, res.size());
        }
        {
            System.out.println(">>> test query with date");
            List<Store> res =
                    manager.getByQualification(
                                    Db.query(Store.class)
                                            .eq("intvalue", new Date(5 * 1000 * 60 * 60 * 24)))
                            .toCacheAndClose();
            res.forEach(o -> System.out.println(o.getName() + " " + o.getIntValue()));
            assertEquals(1, res.size());
        }
    }

    @Test
    public void testDataTypesAlter() throws Exception {
        DbPool pool = createPool("testDataTypesAlter").getPool("test");

        BookStoreSchema schema1 = new BookStoreSchema();
        DbManager manager1 = new DbManagerJdbc("", pool, schema1);

        Store store1 = manager1.inject(new Store());
        store1.setIntValue(Integer.MAX_VALUE);
        store1.setByteValue(Byte.MAX_VALUE);
        store1.setShortValue(Short.MAX_VALUE);
        store1.setCharValue(' ');
        store1.setBigDecimalValue(BigDecimal.TEN);
        store1.setDoubleValue(Double.MAX_VALUE);
        store1.setFloatValue(Float.MAX_VALUE);
        store1.getBlobValue().put("a", "b");
        store1.setSqlDate(new Date(1000));

        store1.save();
        UUID id = store1.getId();

        // alter to String

        BookStoreSchema schema2 = new BookStoreSchema();
        schema2.switchStore = true;
        DbManager manager2 = new DbManagerJdbc("", pool, schema2);
        de.mhus.lib.test.adb.model2.Store store2 =
                manager2.getObject(de.mhus.lib.test.adb.model2.Store.class, id);

        assertEquals(String.valueOf(Integer.MAX_VALUE), store2.getIntValue());
        //		assertEquals(String.valueOf(Byte.MAX_VALUE), store2.getByteValue());
        //		assertEquals(String.valueOf(Short.MAX_VALUE), store2.getShortValue());
        //		assertEquals(" ", store2.getCharValue());
        assertEquals(BigDecimal.TEN.toString(), store2.getBigDecimalValue());
        assertEquals(String.valueOf(Double.MAX_VALUE), store2.getDoubleValue());
        //		assertEquals(String.valueOf(Float.MAX_VALUE), store2.getFloatValue()); //
        // expected:<3.402823[5]E38> but was:<3.402823[4663852886]E38>
        assertEquals("b", store2.getBlobValue().get("a"));
        //		assertEquals("1970-01-01 01:00:01.000000", store2.getSqlDate());
        assertEquals("1970-01-01", store2.getSqlDate().substring(0, 10));

        // and back again

        BookStoreSchema schema3 = new BookStoreSchema();
        DbManager manager3 = new DbManagerJdbc("", pool, schema3);

        Store store3 = manager3.getObject(Store.class, id);

        assertEquals(Integer.MAX_VALUE, store3.getIntValue());
        assertEquals(Byte.MAX_VALUE, store3.getByteValue());
        assertEquals(Short.MAX_VALUE, store3.getShortValue());
        assertEquals(' ', store3.getCharValue());
        assertEquals(BigDecimal.TEN, store3.getBigDecimalValue());
        assertEquals(Double.MAX_VALUE, store3.getDoubleValue());
        assertEquals(Float.MAX_VALUE, store3.getFloatValue());
        assertEquals("b", store3.getBlobValue().get("a"));
        assertEquals(1000, store3.getSqlDate().getTime());
    }
}
