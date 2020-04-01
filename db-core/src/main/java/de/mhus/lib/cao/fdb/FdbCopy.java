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
import java.io.FileFilter;
import java.io.IOException;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.cao.action.CopyConfiguration;
import de.mhus.lib.cao.aspect.Changes;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MFile;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.strategy.Successful;

public class FdbCopy extends CaoAction {

    @Override
    public String getName() {
        return COPY;
    }

    @Override
    public CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException {
        return new CopyConfiguration(null, list, null);
    }

    @Override
    public boolean canExecute(CaoConfiguration configuration) {
        Object to = configuration.getProperties().getProperty(CopyConfiguration.NEW_PARENT);
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
                            configuration.getProperties().getProperty(CopyConfiguration.NEW_PARENT);
            final boolean recursive =
                    configuration.getProperties().getBoolean(CopyConfiguration.RECURSIVE, false);
            CaoNode createdNode = null;
            monitor.setSteps(configuration.getList().size());
            for (CaoNode item : configuration.getList()) {
                monitor.log().d("===", item);
                if (item instanceof FdbNode) {
                    monitor.incrementStep();
                    FdbNode n = (FdbNode) item;

                    File toFile = new File(to.getFile(), n.getFile().getName());

                    ((FdbCore) core).lock();
                    try {
                        MFile.copyDir(
                                n.getFile(),
                                toFile,
                                new FileFilter() {

                                    @Override
                                    public boolean accept(File path) {
                                        if (recursive) return !path.getName().startsWith(".");
                                        //							return path.isFile() &&
                                        // path.getName().startsWith(FdbCore.CONTROL_FILE_PREFIX);
                                        return path.isFile();
                                    }
                                });

                        // remove all ids
                        removeIds(toFile);

                        // index as new
                        ((FdbCore) core).indexFile(toFile);
                        ((FdbCore) core).indexDir(toFile);
                    } finally {
                        ((FdbCore) core).release();
                    }
                    createdNode = new FdbNode((FdbCore) core, toFile, null);

                    Changes change = createdNode.adaptTo(Changes.class);
                    if (change != null) change.deleted();
                }
            }

            return new Successful(getName(), "ok", 0, createdNode);
        } catch (Throwable t) {
            log().d(t);
            return new NotSuccessful(getName(), t.toString(), -1);
        }
    }

    private void removeIds(File file) {
        File meta = ((FdbCore) core).getMetaFileFor(file);
        MProperties p = MProperties.load(meta);
        if (p == null) p = new MProperties();
        p.remove("_id");
        try {
            p.save(meta);
        } catch (IOException e) {
            log().e(file, e);
        }
    }
}
