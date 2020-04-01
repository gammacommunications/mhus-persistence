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
package de.mhus.lib.adb.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbMetadata;
import de.mhus.lib.adb.Persistable;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.annotations.adb.DbPrimaryKey;
import de.mhus.lib.annotations.adb.DbRelation;
import de.mhus.lib.core.MCast;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.pojo.PojoModel;
import de.mhus.lib.core.pojo.PojoParser;
import de.mhus.lib.core.util.MUri;
import de.mhus.lib.errors.MException;

public class AdbUtil {

    public static Class<? extends Persistable> getType(DbManager manager, String typeName)
            throws IOException {
        for (Class<? extends Persistable> item : manager.getSchema().getObjectTypes())
            if (item.getSimpleName().equals(typeName)) {
                return item;
            }
        throw new IOException("Type not found in service: " + typeName);
    }

    public static String getTableName(DbManager manager, String typeName) throws IOException {
        typeName = typeName.toLowerCase();
        for (Class<? extends Persistable> item : manager.getSchema().getObjectTypes())
            if (item.getSimpleName().toLowerCase().equals(typeName)
                    || item.getCanonicalName().toLowerCase().equals(typeName)) {
                return item.getCanonicalName();
            }
        throw new IOException("Type not found in service: " + typeName);
    }

    public static String getTableName(DbManager manager, Class<?> type) throws IOException {
        for (Class<? extends Persistable> item : manager.getSchema().getObjectTypes())
            if (
            /*item.getName().equals(type.getName()) ||*/ item.getCanonicalName()
                    .equals(type.getCanonicalName())) {
                return item.getCanonicalName();
            }
        throw new IOException("Type not found in service: " + type);
    }

    public static Object createAttribute(Class<?> type, Object value) {

        // TODO use escape -ing
        if (value == null || value.equals("[null]")) return null;
        if (value.equals("[uuid]")) return UUID.randomUUID();

        if (value instanceof String) {
            String str = (String) value;
            if (str.startsWith("[") && str.endsWith("]")) {
                String[] parts = str.substring(1, str.length() - 1).split(",");
                for (int i = 0; i < parts.length; i++) parts[i] = MUri.decode(parts[i]);
                value = parts;
            } else {
                value = MUri.decode(str);
            }
        }

        if (type == value.getClass()) return value;

        if (type == int.class || type == Integer.class) return MCast.toint(value, 0);

        if (type == long.class || type == Long.class) return MCast.tolong(value, 0);

        if (type == float.class || type == Float.class) return MCast.tofloat(value, 0);

        if (type == double.class || type == Double.class) return MCast.todouble(value, 0);

        if (type == boolean.class || type == Boolean.class) return MCast.toboolean(value, false);

        if (type == Date.class) return MCast.toDate(String.valueOf(value), null);

        if (type == java.sql.Date.class) {
            Date data = MCast.toDate(String.valueOf(value), null);
            if (data == null) return null;
            return new java.sql.Date(data.getTime());
        }

        if (type == UUID.class) return UUID.fromString(String.valueOf(value));

        if (type.isEnum()) return String.valueOf(value);

        return null;
    }

    public static List<Object> getObjects(DbManager manager, Class<?> type, String id)
            throws MException {
        LinkedList<Object> out = new LinkedList<>();
        if (MString.isEmptyTrim(id)) return out;

        if (id.startsWith("(") && id.endsWith(")")) {
            String aql = id.substring(1, id.length() - 1);
            for (Object o : manager.getByQualification(type, aql, null)) out.add(o);
        } else {
            // TODO separate PKs, currently only one PK is supported
            Object obj = manager.getObject(type, id);
            if (obj != null) out.add(obj);
        }

        return out;
    }

    @Deprecated
    public static void setId(DbMetadata entry, UUID id) {
        if (entry == null) return;
        try {
            Field field = DbMetadata.class.getDeclaredField("id");
            if (!field.canAccess(entry)) field.setAccessible(true);
            field.set(entry, id);
        } catch (Exception e) {
            throw new RuntimeException("Entry " + entry.getClass(), e);
        }
    }

    @Deprecated
    public static void setCreationDate(DbMetadata entry, Date creationDate) {
        if (entry == null) return;
        try {
            Field field = DbMetadata.class.getDeclaredField("creationDate");
            if (!field.canAccess(entry)) field.setAccessible(true);
            field.set(entry, creationDate);
        } catch (Exception e) {
            throw new RuntimeException("Entry " + entry.getClass(), e);
        }
    }

    @Deprecated
    public static void setModifyDate(DbMetadata entry, Date modifyDate) {
        if (entry == null) return;
        try {
            Field field = DbMetadata.class.getDeclaredField("modifyDate");
            if (!field.canAccess(entry)) field.setAccessible(true);
            field.set(entry, modifyDate);
        } catch (Exception e) {
            throw new RuntimeException("Entry " + entry.getClass(), e);
        }
    }

    @Deprecated
    public static void setVstamp(DbMetadata entry, long vstamp) {
        if (entry == null) return;
        try {
            Field field = DbMetadata.class.getDeclaredField("vstamp");
            if (!field.canAccess(entry)) field.setAccessible(true);
            field.set(entry, vstamp);
        } catch (Exception e) {
            throw new RuntimeException("Entry " + entry.getClass(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static PojoModel createDefaultPojoModel(Class<?> clazz) {
        return new PojoParser()
                .parse(
                        clazz,
                        "_",
                        false,
                        new Class[] {DbPersistent.class, DbPrimaryKey.class, DbRelation.class})
                .filter(true, false, true, false, true)
                .getModel();
    }
}
