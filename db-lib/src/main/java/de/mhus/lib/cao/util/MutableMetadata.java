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
package de.mhus.lib.cao.util;

import de.mhus.lib.cao.CaoDriver;
import de.mhus.lib.cao.CaoMetaDefinition;
import de.mhus.lib.cao.CaoMetaDefinition.TYPE;
import de.mhus.lib.cao.CaoMetadata;

public class MutableMetadata extends CaoMetadata {

    public MutableMetadata(CaoDriver driver) {
        super(driver);
    }

    public MutableMetadata addDefinition(CaoMetaDefinition def) {
        CaoMetaDefinition last = index.put(def.getName(), def);
        if (last != null) definition.remove(last);
        definition.add(def);
        return this;
    }

    public MutableMetadata addDefinition(
            String name, TYPE type, String nls, long size, String... categories) {
        addDefinition(new CaoMetaDefinition(this, name, type, nls, size, categories));
        return this;
    }

    public MutableMetadata addDefinition(String name, TYPE type, long size, String... categories) {
        addDefinition(new CaoMetaDefinition(this, name, type, name, size, categories));
        return this;
    }
}
