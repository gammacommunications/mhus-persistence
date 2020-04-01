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
package de.mhus.lib.cao.action;

import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.definition.DefRoot;
import de.mhus.lib.form.definition.FmCheckbox;

public class CopyConfiguration extends CaoConfiguration {

    public CopyConfiguration(CaoConfiguration con, CaoList list, IConfig model) {
        super(con, list, model);
    }

    public static final String NEW_PARENT = "parent";
    public static final String RECURSIVE = "recursive";

    public void setNewParent(CaoNode parent) {
        properties.put(NEW_PARENT, parent);
    }

    public void setRecursive(boolean recursive) {
        properties.setBoolean(RECURSIVE, recursive);
    }

    @Override
    protected IConfig createDefaultModel() {
        return new DefRoot(
                //				new FmText(, "name.name=Name", "name.description=New technical name of the
                // node.")
                new FmCheckbox(
                        RECURSIVE,
                        "recursive.name=Recursive",
                        "recursive.description=Delete also sub structures"));
    }
}
