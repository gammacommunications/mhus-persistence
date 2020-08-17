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
package de.mhus.lib.jpa;

import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.util.MObject;

public abstract class JpaSchema extends MObject {

    public abstract Class<?>[] getObjectTypes();

    /**
     * This should be called after the manager is created.
     *
     * @param manager
     */
    public void doPostInit(JpaManager manager) {}

    /**
     * Overwrite this method to get the configuration object and initialize the schem. It should be
     * called by the creator to initialize the schema before it is given to the manager.
     *
     * @param config
     */
    public void doInit(IConfig config) {}

    public String getSchemaName() {
        return getClass().getSimpleName();
    }
}
