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
package de.mhus.lib.adb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.mhus.lib.adb.model.Field;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.core.util.Table;
import de.mhus.lib.errors.AccessDeniedException;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DbResult;

/**
 * Represents a collection of results.
 *
 * @author mikehummel
 * @param <O>
 */
public class DbCollectionImpl<O> extends MObject implements DbCollection<O> {

    private DbManager manager;
    private DbResult res;
    private DbConnection con;
    private String registryName;
    private O object;
    private O next;
    private boolean recycle = false;
    private boolean hasNext = true;
    private boolean ownConnection;
    private O current;
    private DbPool pool;

    public DbCollectionImpl(
            DbManager manager,
            DbConnection con,
            boolean ownConnection,
            String registryName,
            O object,
            DbResult res)
            throws MException {

        if (registryName == null) {
            Class<?> clazz = manager.getSchema().findClassForObject(object, manager);
            if (clazz == null)
                throw new MException(
                        "class definition not found for object",
                        object.getClass().getCanonicalName(),
                        registryName);
            registryName = clazz.getCanonicalName();
        }

        this.manager = manager;
        this.res = res;
        this.con = con;
        this.registryName = registryName;
        this.object = object;
        this.ownConnection = ownConnection;
        this.pool = manager.getPool();

        nextObject();
    }

    @SuppressWarnings("unchecked")
    private void nextObject() {
        next = null;
        if (!hasNext) return;
        try {
            while (true) {
                try {

                    hasNext = res.next();
                    if (hasNext) {
                        O out = object;
                        if (!recycle) {
                            try {
                                out =
                                        (O)
                                                manager.getSchema()
                                                        .createObject(
                                                                object instanceof Class<?>
                                                                        ? (Class<?>) object
                                                                        : object.getClass(),
                                                                registryName,
                                                                res,
                                                                manager,
                                                                true);
                            } catch (Throwable t) {
                                close();
                                throw new MException(con, t);
                            }
                        }
                        manager.fillObject(registryName, out, con, res);
                        next = out;
                    } else {
                        next = null;
                    }
                    break;

                } catch (AccessDeniedException ade) {
                    // next one
                } catch (Throwable ade) {
                    close();
                    throw ade;
                }
            }
        } catch (Exception e) {
            log().w(e);
            hasNext = false;
        }
        if (!hasNext) close();
    }

    @Override
    public void close() {
        if (res != null) {
            try {
                res.close();
            } catch (Exception e) {
                log().w(e);
            }
            res = null;
            next = null;
            hasNext = false;
            object = null;
        }
        if (con != null) {
            if (ownConnection) manager.getSchema().closeConnection(pool, con);
            con = null;
        }
    }

    /**
     * If recycle is on the original container object will be used for every iteration. If it's off
     * then every time a new object will be created. Default is off.
     *
     * @param on
     * @return x
     */
    @Override
    public DbCollectionImpl<O> setRecycle(boolean on) {
        recycle = on;
        return this;
    }

    @Override
    public boolean isRecycle() {
        return recycle;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public O current() throws MException {
        return current;
    }

    @Override
    public Iterator<O> iterator() {
        return this;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public O next() {
        if (!hasNext) throw new NoSuchElementException();
        current = next;
        nextObject();
        return current;
    }

    /**
     * Transfer Objects to a table view.
     *
     * @param maxSize More the zero, zero or less will disable the parameter
     * @return x
     */
    @Override
    public Table toTableAndClose(int maxSize) {
        Table out = new Table();

        de.mhus.lib.adb.model.Table dbt = manager.getTable(registryName);
        for (Field dbf : dbt.getFields())
            out.addHeader(dbf.getName(), dbf.getType().getCanonicalName());

        Object[] row = new Object[out.getColumnSize()];
        for (O o : this) {
            int cnt = 0;
            try {
                for (Field dbf : dbt.getFields()) {
                    row[cnt] = dbf.get(o);
                    cnt++;
                }
                out.addRow(row);
                if (maxSize > 0 && out.getRowSize() >= maxSize) break;
            } catch (Throwable t) {
                log().d(t, cnt);
            }
        }
        close();

        return out;
    }
}
