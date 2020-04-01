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
package de.mhus.lib.cao.adb;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.cao.action.DeleteConfiguration;
import de.mhus.lib.cao.aspect.Changes;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.strategy.Successful;

public class AdbDelete extends CaoAction {

    @Override
    public String getName() {
        return DELETE;
    }

    @Override
    public CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException {
        return new DeleteConfiguration(null, list, null);
    }

    @Override
    public boolean canExecute(CaoConfiguration configuration) {
        return configuration.getList().size() > 0 && core.containsNodes(configuration.getList());
    }

    @Override
    public OperationResult doExecuteInternal(CaoConfiguration configuration, Monitor monitor)
            throws CaoException {
        if (!canExecute(configuration)) return new NotSuccessful(getName(), "can't execute", -1);

        try {
            boolean deleted = false;
            monitor = checkMonitor(monitor);
            boolean recursive =
                    configuration.getProperties().getBoolean(DeleteConfiguration.RECURSIVE, false);
            monitor.setSteps(configuration.getList().size());
            for (CaoNode item : configuration.getList()) {
                monitor.log().i(">>>", item);
                if (item instanceof AdbNode) {
                    monitor.incrementStep();
                    AdbNode n = (AdbNode) item;
                    if (n.getNodes().size() > 0 && !recursive)
                        monitor.log().i("*** Node is not empty", item);
                    else {
                        monitor.log().d("=== Delete", item);
                        deleted = true;
                        Changes change = item.adaptTo(Changes.class);
                        if (change != null) change.deleted();
                    }
                }

                monitor.log().i("<<<", item);
            }
            if (deleted) return new Successful(getName());
            else return new NotSuccessful(getName(), "no nodes deleted", -1);
        } catch (Throwable t) {
            log().w(t);
            return new NotSuccessful(getName(), t.toString(), -1);
        }
    }
}
