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
package de.mhus.lib.adb.relation;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import de.mhus.lib.adb.DbComfortableObject;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.annotations.adb.DbRelation;
import de.mhus.lib.sql.DbConnection;

/**
 * RelList class.
 *
 * @author mikehummel
 * @version $Id: $Id
 * @param <T>
 */
public class RelList<T> implements List<T> {

    List<T> current;
    List<T> add;
    List<T> remove;
    List<T> org;
    List<T> changed;
    private DbRelation config;

    /**
     * Constructor for RelList.
     *
     * @param list a {@link java.util.List} object.
     * @param config a {@link de.mhus.lib.annotations.adb.DbRelation} object.
     */
    @SuppressWarnings("unchecked")
    public RelList(List<?> list, DbRelation config) {
        org = (List<T>) list;
        current = (List<T>) list;
        this.config = config;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return current.size();
    }
    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return current.isEmpty();
    }
    /** {@inheritDoc} */
    @Override
    public boolean contains(Object o) {
        return current.contains(o);
    }
    /** {@inheritDoc} */
    @Override
    public Iterator<T> iterator() {
        return new LinkedList<T>(current).iterator(); // iterate over a copy
    }
    /** {@inheritDoc} */
    @Override
    public Object[] toArray() {
        return current.toArray();
    }
    /** {@inheritDoc} */
    @Override
    public <E> E[] toArray(E[] a) {
        return current.toArray(a);
    }
    /** {@inheritDoc} */
    @Override
    public boolean add(T e) {
        init();
        if (remove.contains(e)) remove.remove(e);
        if (!org.contains(e) && !add.contains(e)) add.add(e);
        if (!current.contains(e)) current.add(e);
        return true;
    }
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object o) {
        init();
        if (!remove.contains(o) && org.contains(o)) remove.add((T) o);
        if (add.contains(o)) add.remove(o);
        return current.remove(o);
    }

    private void init() {
        if (add != null) return;
        current = new LinkedList<T>(org);
        add = new LinkedList<T>();
        remove = new LinkedList<T>();
        changed = new LinkedList<T>();
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsAll(Collection<?> c) {
        return current.containsAll(c);
    }
    /** {@inheritDoc} */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        for (T t : c) add(t);
        return true;
    }
    /** {@inheritDoc} */
    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        for (T t : c) add(t);
        return true;
    }
    /** {@inheritDoc} */
    @Override
    public boolean removeAll(Collection<?> c) {
        for (Object t : c) remove(t);
        return true;
    }
    /** {@inheritDoc} */
    @Override
    public boolean retainAll(Collection<?> c) {
        return current.retainAll(c);
    }
    /** {@inheritDoc} */
    @Override
    public void clear() {
        for (T t : current) remove(t);
    }
    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        return current.equals(o);
    }
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return current.hashCode();
    }
    /** {@inheritDoc} */
    @Override
    public T get(int index) {
        return current.get(index);
    }
    /** {@inheritDoc} */
    @Override
    public T set(int index, T element) {
        // return current.set(index, element);
        return null;
    }
    /** {@inheritDoc} */
    @Override
    public void add(int index, T element) {
        add(element);
    }
    /** {@inheritDoc} */
    @Override
    public T remove(int index) {
        remove(get(index));
        return null;
    }
    /** {@inheritDoc} */
    @Override
    public int indexOf(Object o) {
        return current.indexOf(o);
    }
    /** {@inheritDoc} */
    @Override
    public int lastIndexOf(Object o) {
        return current.lastIndexOf(o);
    }
    /** {@inheritDoc} */
    @Override
    public ListIterator<T> listIterator() {
        return current.listIterator();
    }
    /** {@inheritDoc} */
    @Override
    public ListIterator<T> listIterator(int index) {
        return current.listIterator(index);
    }
    /** {@inheritDoc} */
    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return current.subList(fromIndex, toIndex);
    }

    /**
     * changed.
     *
     * @return a boolean.
     */
    public boolean changed() {
        return add != null && (add.size() > 0 || remove.size() > 0 || changed.size() > 0);
    }

    /**
     * save.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param fName a {@link java.lang.String} object.
     * @param value a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public void save(DbManager manager, DbConnection con, String fName, Object value)
            throws Exception {
        if (remove != null) {
            for (T t : remove) {
                try {
                    if (config.remove()) {
                        manager.deleteObject(con, t);
                    } else {
                        manager.getTable(manager.getRegistryName(t)).getField(fName).set(t, null);
                        manager.saveObject(con, t);
                    }
                } catch (Throwable te) {
                    te.printStackTrace(); // XXX
                }
            }
        }
        if (add != null) {
            for (T t : add) {
                try {
                    manager.getTable(manager.getRegistryName(t)).getField(fName).set(t, value);
                    if ((t instanceof DbComfortableObject)
                            && !((DbComfortableObject) t).isAdbPersistent())
                        manager.createObject(con, t);
                    else manager.saveObject(con, t);
                } catch (Throwable te) {
                    te.printStackTrace(); // XXX
                }
            }
        }
        if (changed != null) {
            for (T t : changed) {
                try {
                    if (current.contains(t)) {
                        manager.getTable(manager.getRegistryName(t)).getField(fName).set(t, value);
                        //					if (t instanceof DbComfortableObject &&
                        // !((DbComfortableObject)t).isAdbManaged() )
                        manager.saveObject(con, t);
                        //					else
                        //						manager.saveObject(con, t);
                    }
                } catch (Throwable te) {
                    te.printStackTrace(); // XXX
                }
            }
        }

        org = current;
        current = null;
        add = null;
        remove = null;
        changed = null;
    }

    /**
     * Setter for the field <code>changed</code>.
     *
     * @param obj a T object.
     */
    public void setChanged(T obj) {
        init();
        changed.add(obj);
    }

    /**
     * unsetChanged.
     *
     * @param obj a T object.
     */
    public void unsetChanged(T obj) {
        if (changed == null) return;
        changed.remove(obj);
    }
}
