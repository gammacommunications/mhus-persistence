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
package de.mhus.lib.cao.fs;

import java.io.File;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.cao.action.DeleteRenditionConfiguration;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.strategy.Successful;
import de.mhus.lib.errors.MException;

public class FsDeleteRendition extends CaoAction {

    @Override
    public String getName() {
        return DELETE_RENDITION;
    }

    @Override
    public CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException {
        return new DeleteRenditionConfiguration(null, list, null);
    }

    @Override
    public boolean canExecute(CaoConfiguration configuration) {
        try {
            return configuration.getList().size() != 1
                    && configuration.getList().get(0) instanceof FsNode;
        } catch (Throwable t) {
            log().d(t);
            return false;
        }
    }

    @Override
    public OperationResult doExecuteInternal(CaoConfiguration configuration, Monitor monitor)
            throws CaoException {
        if (!canExecute(configuration)) return new NotSuccessful(getName(), "can't execute", -1);

        try {
            FsNode parent = (FsNode) configuration.getList().get(0);

            String rendition =
                    configuration.getProperties().getString(DeleteRenditionConfiguration.RENDITION);
            File renditionFile =
                    ((FsCore) parent.getConnection())
                            .getContentFileFor(parent.getFile(), rendition);
            if (renditionFile == null || !renditionFile.exists() || !renditionFile.isFile())
                throw new MException("rendition not found", rendition);

            if (!renditionFile.delete()) throw new MException("can't delete rendition", rendition);

            return new Successful(getName());
        } catch (Throwable t) {
            log().d(t);
            return new NotSuccessful(getName(), t.toString(), -1);
        }
    }
}
