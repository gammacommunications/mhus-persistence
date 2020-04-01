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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.UUID;

import de.mhus.lib.adb.util.AdbUtil;
import de.mhus.lib.annotations.adb.DbIndex;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.annotations.adb.DbPrimaryKey;
import de.mhus.lib.annotations.adb.DbTable;
import de.mhus.lib.annotations.generic.Public;
import de.mhus.lib.basics.UuidIdentificable;
import de.mhus.lib.basics.consts.GenerateHidden;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.pojo.PojoAttribute;
import de.mhus.lib.core.pojo.PojoModel;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;

@DbTable(features = DbAccessManager.FEATURE_NAME)
public abstract class DbMetadata extends DbComfortableObject
        implements UuidIdentificable, Externalizable {

    @DbPrimaryKey @Public private UUID id;

    @DbPersistent
    @DbIndex("adb_created")
    @Public
    private Date creationDate;

    @DbPersistent
    @DbIndex("adb_modified")
    @Public
    private Date modifyDate;

    @DbPersistent @Public private long vstamp;

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    @GenerateHidden
    public void doPreCreate(DbConnection con) {
        creationDate = new Date();
        modifyDate = creationDate;
        vstamp = 0;
    }

    @Override
    @GenerateHidden
    public void doPreSave(DbConnection con) {
        modifyDate = new Date();
        vstamp++;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public Date getModifyDate() {
        return modifyDate;
    }

    public long getVstamp() {
        return vstamp;
    }

    @GenerateHidden
    public abstract DbMetadata findParentObject() throws MException;

    @Override
    public String toString() {
        return MSystem.toString(this, getId());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        PojoModel model = AdbUtil.createDefaultPojoModel(getClass());
        for (PojoAttribute<?> field : model) {
            if (field.canRead()) {
                out.writeBoolean(true);
                out.writeUTF(field.getName());
                out.writeObject(field.get(this));
            }
        }
        out.writeBoolean(false);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        PojoModel model = AdbUtil.createDefaultPojoModel(getClass());
        while (in.readBoolean()) {
            String name = in.readUTF();
            Object value = in.readObject();
            @SuppressWarnings("rawtypes")
            PojoAttribute attr = model.getAttribute(name);
            if (attr != null) {
                if (attr.canWrite()) attr.set(this, value);
            } else log().d("can't read external", name);
        }
    }
}
