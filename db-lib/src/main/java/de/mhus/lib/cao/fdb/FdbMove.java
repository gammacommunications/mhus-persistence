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
package de.mhus.lib.cao.fdb;

import java.io.File;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.cao.action.MoveConfiguration;
import de.mhus.lib.cao.aspect.Changes;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.strategy.Successful;

public class FdbMove extends CaoAction {

    @Override
    public String getName() {
        return MOVE;
    }

    @Override
    public CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException {
        return new MoveConfiguration(null, list, null);
    }

    @Override
    public boolean canExecute(CaoConfiguration configuration) {
        Object to = configuration.getProperties().getProperty(MoveConfiguration.NEW_PARENT);
        return configuration.getList().size() > 0
                && to instanceof FdbNode
                && core.containsNode((CaoNode) to)
                && core.containsNodes(configuration.getList());
    }

    @Override
    public OperationResult doExecuteInternal(CaoConfiguration configuration, Monitor monitor)
            throws CaoException {
        if (!canExecute(configuration)) return new NotSuccessful(getName(), "can't execute", -1);

        try {
            FdbNode to =
                    (FdbNode)
                            configuration.getProperties().getProperty(MoveConfiguration.NEW_PARENT);
            monitor.setSteps(configuration.getList().size());
            for (CaoNode item : configuration.getList()) {
                monitor.log().d("===", item);
                if (item instanceof FdbNode) {
                    monitor.incrementStep();
                    CaoNode oldParent = null;
                    Changes change = item.adaptTo(Changes.class);
                    if (change != null) oldParent = item.getParent();
                    FdbNode n = (FdbNode) item;
                    File toFile = new File(to.getFile(), n.getFile().getName());
                    ((FdbCore) core).lock();
                    try {
                        n.getFile().renameTo(toFile);
                        ((FdbCore) core).indexFile(toFile);
                        ((FdbCore) core).indexDir(toFile);
                        n.setParent(to);
                        n.reloadById();
                    } finally {
                        ((FdbCore) core).release();
                    }

                    if (change != null) change.movedFrom(oldParent);
                }
            }

            return new Successful(getName(), "ok", 0);
        } catch (Throwable t) {
            log().d(t);
            return new NotSuccessful(getName(), t.toString(), -1);
        }
    }
}
