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

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoActionList;

public class MutableActionList extends CaoActionList {

    public MutableActionList() {}

    public MutableActionList(CaoActionList copyFrom) {
        for (CaoAction action : copyFrom) {
            add(action);
        }
    }

    /**
     * add.
     *
     * @param action a {@link de.mhus.lib.cao.CaoAction} object.
     */
    public void add(CaoAction action) {
        String name = action.getName();

        if (index.containsKey(name)) {
            // TODO find the best one, maybe replace
            return;
        }

        actions.add(action);
        index.put(name, action);
    }
}
