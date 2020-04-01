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
package de.mhus.lib.adb.model;

import java.util.List;

import de.mhus.lib.annotations.adb.DbIndex;
import de.mhus.lib.annotations.adb.DbIndexes;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.annotations.adb.DbPrimaryKey;
import de.mhus.lib.annotations.adb.DbRelation;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.config.MConfig;
import de.mhus.lib.core.directory.WritableResourceNode;
import de.mhus.lib.core.pojo.PojoAttribute;
import de.mhus.lib.core.pojo.PojoModel;

public class TableAnnotations extends Table {

    @Override
    protected void parseFields() throws Exception {

        PojoModel model = manager.getSchema().createPojoModel(clazz);

        for (PojoAttribute<?> attribute : model) {
            String mName = attribute.getName();
            DbPrimaryKey pk = attribute.getAnnotation(DbPrimaryKey.class);
            DbPersistent p = attribute.getAnnotation(DbPersistent.class);
            DbIndex idx = attribute.getAnnotation(DbIndex.class);
            DbRelation r = attribute.getAnnotation(DbRelation.class);

            if (pk != null || p != null || r != null) {

                if (!attribute.canRead()) {
                    log().d("getter not found", mName);
                    continue;
                }
                if (!attribute.canWrite() && r == null) { // relations do not need setters
                    log().d("setter not found", mName);
                    continue;
                }

                if (r != null) {
                    log().t("relation", mName);

                    // check for doubled
                    if (getFieldRelation(mName) != null || getField(mName) != null) {
                        log().t("double field definition", mName);
                        continue;
                    }

                    FieldRelation fr = new FieldRelation(manager, this, attribute, r);
                    addField(fr);

                } else {
                    log().t("field", mName);
                    WritableResourceNode<?> attr = MConfig.toConfig(toAttributes(p, pk));
                    boolean v = (p != null && p.virtual());

                    // check for doubled
                    if (getField(mName) != null || getFieldRelation(mName) != null) {
                        log().t("double field definition", mName);
                        continue;
                    }

                    Field field =
                            manager.getSchema()
                                    .createField(
                                            manager,
                                            this,
                                            pk != null,
                                            p != null && p.ro(),
                                            v,
                                            attribute,
                                            attr,
                                            null,
                                            p != null ? p.features() : null);
                    if (field != null) addField(field);

                    // indexes
                    if (idx != null && field.isPersistent()) {
                        addToIndex(idx.value(), idx.type(), idx.hints(), field);
                    }
                }
            }
        }

        // parse global indexes
        List<DbIndexes> aIndexesList = MSystem.findAnnotations(clazz, DbIndexes.class);
        for (DbIndexes aIndexes : aIndexesList) {
            for (DbIndex aIndex : aIndexes.values()) {
                for (String fieldName : aIndex.fields()) {
                    Field field = getField(fieldName);
                    addToIndex(aIndex.value(), aIndex.type(), aIndex.hints(), field);
                }
                if (aIndex.fields().length == 0)
                    addToIndex(aIndex.value(), aIndex.type(), aIndex.hints(), null);
            }
        }
    }
}
