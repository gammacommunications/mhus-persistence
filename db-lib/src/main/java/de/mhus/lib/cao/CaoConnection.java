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

import java.util.List;

import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.basics.Named;
import de.mhus.lib.core.directory.MResourceProvider;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.WrongStateException;

public abstract class CaoConnection extends MResourceProvider<CaoNode> implements Named {

    protected boolean shared;
    protected boolean closed;

    public abstract CaoDriver getDriver();

    /**
     * Request the first resource.
     *
     * @return The root or null
     */
    public abstract CaoNode getRoot();

    public abstract CaoActionList getActions();

    public abstract <T extends CaoAspect> CaoAspectFactory<T> getAspectFactory(Class<T> ifc);

    @Override
    public abstract String getName();

    /**
     * Send a query into the data store system. This method opens the possibility to access more
     * then the content structure. But it's not specified and must be known by the caller. e.g. get
     * access to the underlying type or user system or send proprietary queries to the system.
     *
     * @param space The data space, e.g. users, types, content
     * @param query The query itself
     * @return A list of results.
     * @throws MException Throws NotSupportedException if the method is not implemented at all
     */
    public abstract CaoList executeQuery(String space, String query) throws MException;

    /**
     * Send a query into the data store system. This method opens the possibility to access more
     * then the content structure. But it's not specified and must be known by the caller. e.g. get
     * access to the underlying type or user system or send proprietary queries to the system.
     *
     * @param space The data space, e.g. users, types, content
     * @param query The query itself
     * @return A list of results.
     * @throws MException Throws NotSupportedException if the method is not implemented at all
     */
    public abstract <T> CaoList executeQuery(String space, AQuery<T> query) throws MException;

    public abstract CaoAction getAction(String name);

    /**
     * Return if the given node is managed by this connection.
     *
     * @param node The node to test
     * @return True if the node has this connection or is manageable by this connection.
     */
    public boolean containsNode(CaoNode node) {
        checkState();
        return this.equals(node.getConnection());
    }

    protected void checkState() {
        if (isClosed()) throw new WrongStateException(getName(), "already closed");
    }

    public boolean containsNodes(List<CaoNode> list) {
        checkState();
        for (CaoNode n : list) if (!containsNode(n)) return false;
        return true;
    }

    public synchronized void close() {
        if (isClosed() || isShared()) return;
        try {
            closeConnection();
        } catch (Throwable t) {
            log().e(getName(), "on close", t);
        }
        closed = true;
    }

    protected abstract void closeConnection() throws Exception;

    public boolean isShared() {
        return shared;
    }

    public boolean isClosed() {
        return closed;
    }
}
