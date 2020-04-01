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
package de.mhus.lib.sql;

import java.util.List;

import de.mhus.lib.cao.CaoDriver;
import de.mhus.lib.cao.CaoMetaDefinition;
import de.mhus.lib.cao.CaoMetadata;

/**
 * A mutable variant of the Metadata to rapid develop extensions.
 *
 * @author mikehummel
 */
public class MutableMetadata extends CaoMetadata {

    public MutableMetadata() {
        super(null); // TODO check !
    }

    public MutableMetadata(CaoDriver driver) {
        super(driver);
    }

    /**
     * This method cleanup the internal index. Manipulate the map before you call a getter, this
     * will recreate the internal index. Changes after it will not affect.
     *
     * @return x
     */
    public List<CaoMetaDefinition> getMap() {
        synchronized (this) {
            index = null;
            return definition;
        }
    }
}
