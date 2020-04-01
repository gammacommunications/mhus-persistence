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
package de.mhus.lib.cao.auth;

import de.mhus.lib.cao.CaoActionStarter;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.util.MNls;

public class AuthActionStarter extends CaoActionStarter {

    private CaoActionStarter starter;

    public AuthActionStarter(CaoActionStarter starter) {
        super(null, null);
        this.starter = starter;
    }

    @Override
    public String getName() {
        return starter.getName();
    }

    @Override
    public boolean canExecute() {
        return starter.canExecute();
    }

    @Override
    public OperationResult doExecute(Monitor monitor) throws CaoException {
        return starter.doExecute(monitor);
    }

    @Override
    public MNls getResourceBundle() {
        return starter.getResourceBundle();
    }

    @Override
    public boolean equals(Object obj) {
        return starter.equals(obj);
    }

    @Override
    public String toString() {
        return starter.toString();
    }
}
