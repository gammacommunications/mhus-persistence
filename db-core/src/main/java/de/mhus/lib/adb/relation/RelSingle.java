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

import java.util.List;

import de.mhus.lib.adb.IRelationObject;
import de.mhus.lib.adb.model.Field;
import de.mhus.lib.adb.model.FieldRelation;
import de.mhus.lib.core.parser.AttributeMap;
import de.mhus.lib.sql.DbConnection;

public class RelSingle<T> implements IRelationObject {

    private FieldRelation field;
    private Object obj;
    private T relation;
    private boolean changed = false;

    @SuppressWarnings("unchecked")
    public T getRelation() throws Exception {
        synchronized (this) {
            if (relation == null) {

                String src = field.getConfig().sourceAttribute();
                if ("".equals(src)) src = field.getName() + "id";
                src = src.toLowerCase();
                String tar = field.getConfig().targetAttribute();
                if ("".equals(tar)) tar = "id";
                // tar = tar.toLowerCase();

                Field idField = field.getTable().getField(src);
                if (idField == null) return null;
                Object id = idField.getFromTarget(obj);
                if (id == null) return null;

                List<?> res =
                        field.getManager()
                                .getByQualification(
                                        field.getConfig().target(),
                                        "$db."
                                                + field.getManager()
                                                        .getMappingName(field.getConfig().target())
                                                + "."
                                                + tar
                                                + "$ = $id$",
                                        new AttributeMap("id", id))
                                .toCacheAndClose();

                if (res != null && res.size() > 0) relation = (T) res.get(0);

                // relation = (T) field.getManager().getObject(field.getConfig().target(), id);

            }
        }
        changed = false;
        return relation;
    }

    public void setRelation(T relation) {
        changed = true;
        this.relation = relation;
    }

    public void reset() {
        synchronized (this) {
            relation = null;
        }
    }

    protected void prepare() throws Exception {
        if (!field.getConfig().managed() || !isChanged()) return;
        synchronized (this) {
            String src = field.getConfig().sourceAttribute();
            if ("".equals(src)) src = field.getName() + "id";
            src = src.toLowerCase();

            Field idField = field.getTable().getField(src);
            if (idField == null) return;

            if (relation == null) idField.set(obj, null);
            else {
                String tar = field.getConfig().targetAttribute();
                if ("".equals(tar)) tar = "id";
                tar = tar.toLowerCase();

                Object id =
                        field.getManager()
                                .getTable(field.getManager().getRegistryName(relation))
                                .getField(tar)
                                .get(relation);
                idField.set(obj, id);
            }
            changed = false;
        }
    }

    @Override
    public void prepareCreate() throws Exception {
        prepare();
    }

    @Override
    public void created(DbConnection con) throws Exception {}

    @Override
    public void saved(DbConnection con) throws Exception {}

    @Override
    public void setManager(FieldRelation field, Object obj) {
        this.field = field;
        this.obj = obj;
    }

    @Override
    public boolean isChanged() {
        return changed;
    }

    @Override
    public void loaded(DbConnection con) {
        synchronized (this) {
            relation = null;
            changed = false;
        }
    }

    @Override
    public void prepareSave(DbConnection con) throws Exception {
        prepare();
    }
}
