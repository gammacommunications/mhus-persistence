/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.sql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SqlMetadata implements Iterable<SqlMetaDefinition> {

    protected LinkedList<SqlMetaDefinition> definition = new LinkedList<SqlMetaDefinition>();
    protected HashMap<String, SqlMetaDefinition> index = new HashMap<String, SqlMetaDefinition>();

    // private CaoDriver driver;

    //    public CaoMetadata(CaoDriver driver) {
    //        this.driver = driver;
    //    }

    @Override
    public Iterator<SqlMetaDefinition> iterator() {
        return definition.iterator();
    };

    public int getCount() {
        return definition.size();
    }

    public SqlMetaDefinition getDefinitionAt(int index) {
        return definition.get(index);
    }

    //    public final CaoDriver getDriver() {
    //        return driver;
    //    }

    public SqlMetaDefinition getDefinition(String name) {
        synchronized (this) {
            if (index.size() == 0) {
                for (SqlMetaDefinition d : this) {
                    index.put(d.getName(), d);
                }
            }
        }
        return index.get(name);
    }

    public List<SqlMetaDefinition> getDefinitionsWithCategory(String category) {
        LinkedList<SqlMetaDefinition> out = new LinkedList<SqlMetaDefinition>();
        for (SqlMetaDefinition meta : this) {
            if (meta.hasCategory(category)) out.add(meta);
        }
        return out;
    }
}
