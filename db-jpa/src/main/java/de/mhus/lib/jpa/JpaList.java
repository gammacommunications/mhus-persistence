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
package de.mhus.lib.jpa;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class JpaList<T> implements List<T> {

    private JpaEntityManager entityManager;
    private List<? extends Object> list;

    public JpaList(JpaEntityManager entityManager, List<? extends Object> list) {
        this.entityManager = entityManager;
        this.list = list;
    }

    @Override
    public boolean add(Object arg0) {
        return false;
    }

    @Override
    public void add(int arg0, Object arg1) {}

    @Override
    public boolean addAll(Collection<? extends T> arg0) {
        return false;
    }

    @Override
    public boolean addAll(int arg0, Collection<? extends T> arg1) {
        return false;
    }

    @Override
    public void clear() {}

    @Override
    public boolean contains(Object arg0) {
        return list.contains(arg0);
    }

    @Override
    public boolean containsAll(Collection<?> arg0) {
        return list.containsAll(arg0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(int arg0) {
        return (T) entityManager.injectObject(list.get(arg0));
    }

    @Override
    public int indexOf(Object arg0) {
        return list.indexOf(arg0);
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return new JpaIterator<T>(entityManager, list.iterator());
    }

    @Override
    public int lastIndexOf(Object arg0) {
        return list.lastIndexOf(arg0);
    }

    @Override
    public ListIterator<T> listIterator() {
        return null;
    }

    @Override
    public ListIterator<T> listIterator(int arg0) {
        return null;
    }

    @Override
    public boolean remove(Object arg0) {
        return false;
    }

    @Override
    public T remove(int arg0) {
        return null;
    }

    @Override
    public boolean removeAll(Collection<?> arg0) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> arg0) {
        return false;
    }

    @Override
    public T set(int arg0, Object arg1) {
        return null;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public List<T> subList(int arg0, int arg1) {
        return new JpaList<T>(entityManager, list.subList(arg0, arg1));
    }

    @Override
    public Object[] toArray() {
        Object[] out = list.toArray();
        if (out != null) for (Object o : out) entityManager.injectObject(o);
        return out;
    }

    @Override
    public <E> E[] toArray(E[] arg0) {
        E[] out = list.toArray(arg0);
        if (out != null) for (Object o : out) entityManager.injectObject(o);
        return out;
    }
}
