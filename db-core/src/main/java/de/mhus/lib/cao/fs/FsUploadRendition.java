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
import de.mhus.lib.cao.action.UploadRenditionConfiguration;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MFile;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.strategy.Successful;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotFoundException;

public class FsUploadRendition extends CaoAction {

    @Override
    public String getName() {
        return UPLOAD_RENDITION;
    }

    @Override
    public CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException {
        return new UploadRenditionConfiguration(null, list, null);
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
                    configuration.getProperties().getString(UploadRenditionConfiguration.RENDITION);
            String path =
                    configuration.getProperties().getString(UploadRenditionConfiguration.FILE);
            File file = new File(path);
            if (!file.exists() || !file.isFile()) throw new NotFoundException(path);

            File renditionFile =
                    ((FsCore) parent.getConnection())
                            .getContentFileFor(parent.getFile(), rendition);
            if (renditionFile == null)
                throw new MException("can't create rendition to internal file", rendition);
            MFile.copyFile(file, renditionFile);

            MProperties p = new MProperties(configuration.getProperties());
            p.remove(UploadRenditionConfiguration.RENDITION);
            p.remove(UploadRenditionConfiguration.FILE);
            p.save(new File(renditionFile.getParentFile(), renditionFile.getName() + ".meta"));

            return new Successful(getName());
        } catch (Throwable t) {
            log().d(t);
            return new NotSuccessful(getName(), t.toString(), -1);
        }
    }
}
