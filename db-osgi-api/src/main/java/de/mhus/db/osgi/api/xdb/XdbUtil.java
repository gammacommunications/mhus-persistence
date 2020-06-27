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
package de.mhus.db.osgi.api.xdb;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.mhus.lib.adb.DbCollection;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.core.MCast;
import de.mhus.lib.core.MCollection;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotFoundException;
import de.mhus.lib.xdb.XdbService;
import de.mhus.lib.xdb.XdbType;
import de.mhus.osgi.api.MOsgi;
import de.mhus.osgi.api.MOsgi.Service;

public class XdbUtil {

    private static final int PAGE_SIZE = 100; //XXX ?

    public static XdbApi getApi(String apiName) throws NotFoundException {
        XdbApi api = MOsgi.getService(XdbApi.class, "(xdb.type=" + apiName + ")");
        if (api == null) throw new NotFoundException("Command API not found", apiName);
        return api;
    }

    public static List<String> getApis() {
        LinkedList<String> out = new LinkedList<>();
        for (Service<XdbApi> s : MOsgi.getServiceRefs(XdbApi.class, null))
            out.add(String.valueOf(s.getReference().getProperty("xdb.type")));
        return out;
    }

    public static <T> DbCollection<T> createObjectList(
            XdbType<T> type, String search, Map<String, Object> parameterValues) throws Exception {

        if (search.startsWith("(") && search.endsWith(")"))
            return type.getByQualification(
                    search.substring(1, search.length() - 1), parameterValues);

        return new IdArrayCollection<T>(type, search.split(","));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void setValue(XdbType<?> type, Object object, String name, Object v)
            throws Exception {
        int p = name.indexOf('.');
        if (p > 0) {
            String p1 = name.substring(0, p);
            String p2 = name.substring(p + 1);
            Class<?> t = type.getAttributeType(p1);
            if (Map.class.isAssignableFrom(t)) {
                Map map = type.get(object, p1);
                if (map == null) {
                    if (t.isInterface()) map = new HashMap<>();
                    else map = (Map) t.getDeclaredConstructor().newInstance();
                    type.set(object, p1, map);
                }
                if (p2.equals("remove")) {
                    map.remove(v);
                } else if (p2.startsWith("set:")) {
                    p2 = p2.substring(4);
                    map.put(p2, v);
                } else map.put(p2, v);
            } else if (Collection.class.isAssignableFrom(t)) {
                Collection col = type.get(object, p1);
                if (col == null) {
                    if (t.isInterface()) col = new LinkedList<>();
                    else col = (Collection) t.getDeclaredConstructor().newInstance();
                    type.set(object, p1, col);
                }
                if (p2.equals("add") || p2.equals("last")) {
                    col.add(v);
                } else if (p2.equals("first") && col instanceof Deque) {
                    ((Deque) col).addFirst(v);
                } else if (p2.equals("clear")) {
                    col.clear();
                } else if (p2.equals("remove")) {
                    col.remove(v);
                } else {
                    int i = MCast.toint(p2, -1);
                    if (i > -1 && col instanceof AbstractList) {
                        ((AbstractList) col).set(i, v);
                    }
                }

            } else if (t.isArray()) {
                Object array = type.get(object, p1);
                LinkedList<Object> col = (LinkedList<Object>) MCollection.toList((Object[]) array);
                if (p2.equals("add") || p2.equals("last")) {
                    col.add(v);
                } else if (p2.equals("first")) {
                    col.addFirst(v);
                } else if (p2.equals("clear")) {
                    col.clear();
                } else if (p2.equals("remove")) {
                    col.remove(v);
                } else {
                    int i = MCast.toint(p2, -1);
                    if (i > -1) {
                        col.set(i, v);
                    }
                }
                array = Array.newInstance(t.getComponentType(), col.size());
                for (int i = 0; i < col.size(); i++) Array.set(array, i, col.get(i));
                type.set(object, p1, array);
            }
        } else type.set(object, name, v);
    }

    public static Object prepareValue(XdbType<?> type, String name, Object value) {
        int p = name.indexOf('.');
        if (p > 0) return value;
        Object v = type.prepareManualValue(name, value);
        return v;
    }

    public static <T> LinkedList<T> collectResults(XdbService manager, AQuery<T> query, int page) throws MException {
        LinkedList<T> list = new LinkedList<T>();
        DbCollection<T> res = manager.getByQualification(query);
        if (!res.skip(page * PAGE_SIZE)) return list;
        while (res.hasNext()) {
            list.add(res.next());
            if (list.size() >= PAGE_SIZE) break;
        }
        res.close();
        return list;
    }

}
