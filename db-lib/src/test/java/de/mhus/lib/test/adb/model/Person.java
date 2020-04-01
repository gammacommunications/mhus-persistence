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

import de.mhus.lib.adb.Persistable;
import de.mhus.lib.adb.relation.RelMultible;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.annotations.adb.DbPrimaryKey;
import de.mhus.lib.annotations.adb.DbRelation;

public class Person implements Persistable {

    private UUID id;
    private String name;
    private RelMultible<Book> lendTo = new RelMultible<Book>();

    @DbPrimaryKey
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    @DbPersistent
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @DbRelation(target = Book.class)
    public RelMultible<Book> getLendTo() {
        return lendTo;
    }

    @Override
    public String toString() {
        return "Person " + name;
    }
}
