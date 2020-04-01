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
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.core.strategy.DefaultMonitor;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.util.MNls;

/**
 * A action is doing something with one or more objects. The main difference to a operation is that
 * the objects do not have meanings. A action can call a operation to execute.
 *
 * @author mikehummel
 */
public abstract class CaoAction extends MObject {

    public static final String UPDATE = "updateNode";
    public static final String DELETE = "deleteNode";
    public static final String MOVE = "moveNode";
    public static final String RENAME = "renameNode";
    public static final String CREATE = "createNode";
    public static final String UPLOAD_RENDITION =
            "uploadRendition"; // CREATE or UPDATE; CREATE would suggest to generate from default
                               // rendition, e.g. create a thumbnail
    public static final String DELETE_RENDITION = "deleteRendition";
    public static final String COPY = "copyNode";

    private MNls resourceBundle;
    protected CaoCore core;

    public void doInitialize(CaoCore core) {
        if (this.core != null) return;
        this.core = core;
        resourceBundle = MNls.lookup(this);
    }

    public abstract String getName();

    /**
     * @param node
     * @param configuration
     * @return configuration object
     * @throws CaoException
     */
    public CaoConfiguration createConfiguration(CaoNode node, IProperties configuration)
            throws CaoException {
        return createConfiguration(new CaoList(null, node), configuration);
    }

    /**
     * Returns a configuration Form for the operation. The list of elements should be a
     * representative list. The configuration use most time the first element of this list. In other
     * cases the hole list is needed, it depends on the operation.
     *
     * @param list
     * @param configuration specific initial attributes
     * @return x
     * @throws CaoException
     */
    public abstract CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException;

    public abstract boolean canExecute(CaoConfiguration configuration);

    /**
     * Executes a defined action. Is the action need to execute an operation it will return the
     * operation object to be executed by the caller.
     *
     * @param configuration
     * @param monitor
     * @return x
     * @throws CaoException
     */
    public final OperationResult doExecute(CaoConfiguration configuration, Monitor monitor)
            throws CaoException {
        if (monitor == null) monitor = new DefaultMonitor(getClass());
        monitor.log().d(">>>", configuration.getProperties());
        OperationResult res = doExecuteInternal(configuration, monitor);
        monitor.log().d("<<<", res);
        return res;
    }

    public final OperationResult doExecute(CaoConfiguration configuration) throws CaoException {
        return doExecute(configuration, null);
    }

    protected abstract OperationResult doExecuteInternal(
            CaoConfiguration configuration, Monitor monitor) throws CaoException;

    @Override
    public String toString() {
        return "Action " + getName() + " (" + getClass().getCanonicalName() + ")";
    }

    public MNls getResourceBundle() {
        return resourceBundle;
    }

    protected Monitor checkMonitor(Monitor monitor) {
        if (monitor == null) monitor = new DefaultMonitor(getClass());
        return monitor;
    }
}
