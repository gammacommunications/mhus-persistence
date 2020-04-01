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

import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.util.MNls;

public class CaoActionStarter {

    private CaoAction action;
    private CaoConfiguration config;

    public CaoActionStarter(CaoAction action, CaoConfiguration config) {
        this.action = action;
        this.config = config;
    }

    public String getName() {
        return action.getName();
    }

    public boolean canExecute() {
        return action.canExecute(config);
    }

    public OperationResult doExecute(Monitor monitor) throws CaoException {
        if (monitor == null) monitor = action.checkMonitor(monitor);
        return action.doExecute(config, monitor);
    }

    public MNls getResourceBundle() {
        return action.getResourceBundle();
    }
}
