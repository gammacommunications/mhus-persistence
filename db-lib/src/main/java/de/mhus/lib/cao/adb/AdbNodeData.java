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
package de.mhus.lib.cao.adb;

import java.util.UUID;

import de.mhus.lib.adb.DbMetadata;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.errors.MException;

public class AdbNodeData extends DbMetadata {

    public enum TYPE {
        NODE,
        LEAF
    };

    @DbPersistent(size = 50)
    private String collection;

    @DbPersistent private UUID parent;
    @DbPersistent private String name;
    @DbPersistent private TYPE type;
    @DbPersistent private MProperties properties;

    @Override
    public DbMetadata findParentObject() throws MException {
        return null;
    }

    public UUID getParent() {
        return parent;
    }

    public void setParent(UUID parent) {
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TYPE getType() {
        return type;
    }

    public void setType(TYPE type) {
        this.type = type;
    }

    public String getCollection() {
        return collection;
    }

    public MProperties getProperties() {
        return properties;
    }

    public void setProperties(MProperties modified) {
        this.properties = modified;
    }
}
