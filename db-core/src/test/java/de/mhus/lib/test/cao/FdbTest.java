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
package de.mhus.lib.test.cao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mhus.lib.cao.CaoConnection;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.aspect.StructureControl;
import de.mhus.lib.cao.fdb.FdbCore;
import de.mhus.lib.cao.util.DefaultStructureControl;
import de.mhus.lib.core.MApi;
import de.mhus.lib.core.MFile;
import de.mhus.lib.core.logging.Log.LEVEL;

public class FdbTest {

    private FdbCore core;
    private File to;
    private CaoConnection con;

    @BeforeAll
    public static void begin() {
        try {
            MApi.get().getLogFactory().setDefaultLevel(LEVEL.DEBUG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createEnvironment(boolean useCache) throws Exception {
        to = new File("target/caotest");
        MFile.deleteDir(to);
        File from = new File("src/test/fdb");
        MFile.copyDir(from, to);
        core = new FdbCore("test", to, useCache);
        con = core.getConnection();
    }

    @Test
    public void testGeneral() throws Exception {
        createEnvironment(false);
        core.registerAspectFactory(StructureControl.class, new DefaultStructureControl("sort"));

        // check if index was created
        assertEquals(
                true,
                new File(to, "repository/index/04115234/aad1/4e0e/a0aa/2f887a6e672a").exists());

        // get Node by path
        {
            CaoNode n = con.getResourceByPath("/page/sub1");
            System.out.println(n);
            assertNotNull(n);
            assertEquals("sub1", n.getName());
        }
        // get node by id
        {
            CaoNode n = con.getResourceById("1503806b-ee68-4e81-b408-8ef2972bdafe");
            System.out.println(n);
            assertNotNull(n);
            assertEquals("sub1", n.getName());
        }

        // rename
        {
            CaoNode n = con.getResourceByPath("/page/sub2");
            StructureControl control = n.adaptTo(StructureControl.class);
            boolean res = control.rename("bingo");
            assertTrue(res);
            assertEquals("bingo", n.getName());
            assertEquals("/page/bingo", n.getPath());
            assertEquals(n.getId(), core.getResourceByPath("/page/bingo").getId());
        }
    }

    @Test
    public void testMove() throws Exception {
        createEnvironment(false);
        core.registerAspectFactory(StructureControl.class, new DefaultStructureControl("sort"));

        CaoNode f = core.getResourceByPath("/page/sub1");
        System.out.println(f);
        assertNotNull(f);

        CaoNode t = core.getResourceByPath("/html");
        System.out.println(t);
        assertNotNull(t);

        StructureControl control = f.adaptTo(StructureControl.class);
        boolean res = control.moveTo(t);
        assertEquals(true, res);

        // check file
        assertEquals(true, new File(to, "repository/files/html/sub1").exists());

        // check file
        assertEquals(false, new File(to, "repository/files/page/sub1").exists());

        // check node
        assertEquals("/html/sub1", f.getPath());
        assertEquals("/html/sub1", con.getResourceById(f.getId()).getPath());
    }

    @Test
    public void testCopy() throws Exception {
        createEnvironment(false);
        core.registerAspectFactory(StructureControl.class, new DefaultStructureControl("sort"));

        CaoNode f = core.getResourceByPath("/page/sub1");
        System.out.println(f);
        assertNotNull(f);

        CaoNode t = core.getResourceByPath("/html");
        System.out.println(t);
        assertNotNull(t);

        StructureControl control = f.adaptTo(StructureControl.class);
        CaoNode res = control.copyTo(t, true);
        assertNotNull(res);

        // check file
        assertEquals(true, new File(to, "repository/files/html/sub1").exists());
        assertEquals(true, new File(to, "repository/files/page/sub1").exists());

        // check nodes
        assertEquals("/page/sub1", f.getPath());
        assertEquals("/page/sub1", con.getResourceById(f.getId()).getPath());

        assertEquals("/html/sub1", res.getPath());
        assertEquals("/html/sub1", con.getResourceById(res.getId()).getPath());

        // check sub structure
        assertNotNull(con.getResourceByPath("/html/sub1/sub1sub1"));
    }

    @Test
    public void testLock() throws Exception {
        createEnvironment(false);

        core.lock();
        try {
            try {
                core.lock(100);
                throw new Exception("lock not working");
            } catch (TimeoutException e) {
                System.out.println(e.toString());
            }
        } finally {
            core.release();
        }

        core.release();
        core.release();

        core.lock();
        try {
            try {
                core.lock(100);
                throw new Exception("lock not working");
            } catch (TimeoutException e) {
                System.out.println(e.toString());
            }
        } finally {
            core.release();
        }
    }

    @Test
    public void testReorder() throws Exception {
        createEnvironment(false);
        core.registerAspectFactory(StructureControl.class, new DefaultStructureControl("sort"));

        {
            CaoNode n = core.getResourceByPath("/page/sub1/sub1sub1");
            StructureControl control = n.adaptTo(StructureControl.class);
            assertEquals(0, control.getPositionIndex());
        }
        {
            CaoNode n = core.getResourceByPath("/page/sub1/sub1sub4");
            StructureControl control = n.adaptTo(StructureControl.class);
            assertEquals(3, control.getPositionIndex());
        }
        {
            CaoNode n = core.getResourceByPath("/page/sub1/_content");
            StructureControl control = n.adaptTo(StructureControl.class);
            assertEquals(4, control.getPositionIndex());
        }
        {
            CaoNode n = core.getResourceByPath("/page/sub1/sub1sub2");
            System.out.println(n);
            assertNotNull(n);

            StructureControl control = n.adaptTo(StructureControl.class);

            assertEquals(1, control.getPositionIndex());

            control.moveDown();
            assertEquals(2, control.getPositionIndex());

            control.moveUp();
            assertEquals(1, control.getPositionIndex());

            control.moveToTop();
            assertEquals(0, control.getPositionIndex());

            control.moveToBottom();
            assertEquals(4, control.getPositionIndex());

            CaoNode x = core.getResourceByPath("/page/sub1/sub1sub1");
            control.moveAfter(x);
            assertEquals(1, control.getPositionIndex());

            x = core.getResourceByPath("/page/sub1/sub1sub1");
            control.moveBefore(x);
            assertEquals(0, control.getPositionIndex());

            // move after and change parent same time
            x = core.getResourceByPath("/page/sub1");
            assertNotSame(x.getParentId(), n.getParentId());
            control.moveAfter(x);
            assertEquals(x.getParentId(), n.getParentId());
        }
    }
}
