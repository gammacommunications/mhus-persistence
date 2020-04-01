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
package de.mhus.lib.cao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CaoMetadata implements Iterable<CaoMetaDefinition> {

    protected LinkedList<CaoMetaDefinition> definition = new LinkedList<CaoMetaDefinition>();
    protected HashMap<String, CaoMetaDefinition> index = new HashMap<String, CaoMetaDefinition>();

    private CaoDriver driver;

    public CaoMetadata(CaoDriver driver) {
        this.driver = driver;
    }

    @Override
    public Iterator<CaoMetaDefinition> iterator() {
        return definition.iterator();
    };

    public int getCount() {
        return definition.size();
    }

    public CaoMetaDefinition getDefinitionAt(int index) {
        return definition.get(index);
    }

    public final CaoDriver getDriver() {
        return driver;
    }

    public CaoMetaDefinition getDefinition(String name) {
        synchronized (this) {
            if (index.size() == 0) {
                for (CaoMetaDefinition d : this) {
                    index.put(d.getName(), d);
                }
            }
        }
        return index.get(name);
    }

    public List<CaoMetaDefinition> getDefinitionsWithCategory(String category) {
        LinkedList<CaoMetaDefinition> out = new LinkedList<CaoMetaDefinition>();
        for (CaoMetaDefinition meta : this) {
            if (meta.hasCategory(category)) out.add(meta);
        }
        return out;
    }
}
