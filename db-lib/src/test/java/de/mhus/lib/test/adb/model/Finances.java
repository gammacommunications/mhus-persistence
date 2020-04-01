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
package de.mhus.lib.test.adb.model;

import java.util.UUID;

import de.mhus.lib.adb.DbComfortableObject;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.annotations.adb.DbPrimaryKey;
import de.mhus.lib.annotations.adb.DbTable;

@DbTable(features = "accesscontrol")
public class Finances extends DbComfortableObject {

    private UUID id;
    private UUID store;
    private double activa;
    private double passiva;
    private String confidential;
    private String newConfidential;

    @DbPrimaryKey
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    @DbPersistent
    public double getActiva() {
        return activa;
    }

    public void setActiva(double activa) {
        this.activa = activa;
    }

    @DbPersistent
    public double getPassiva() {
        return passiva;
    }

    public void setPassiva(double passiva) {
        this.passiva = passiva;
    }

    @DbPersistent
    public void setStore(UUID shop) {
        this.store = shop;
    }

    public UUID getStore() {
        return store;
    }

    @DbPersistent
    public void setConfidential(String confidential) {
        this.confidential = confidential;
        this.newConfidential = null;
    }

    public String getConfidential() {
        return confidential;
    }

    public void setNewConfidential(String string) {
        newConfidential = string;
    }

    public String getNewConfidential() {
        return newConfidential;
    }
}
