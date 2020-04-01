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

import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.errors.MException;

public class ConnectionAdapter extends CaoConnection {

    private CaoCore core;

    public ConnectionAdapter(CaoCore core) {
        this.core = core;
    }

    public CaoConnection getConnection() {
        return core.getConnection();
    }

    @Override
    public String toString() {
        return core.toString();
    }

    @Override
    public CaoNode getResourceByPath(String path) {
        return core.getResourceByPath(path);
    }

    @Override
    public CaoNode getResourceById(String id) {
        return core.getResourceById(id);
    }

    @Override
    public CaoDriver getDriver() {
        return core.getDriver();
    }

    @Override
    public CaoNode getRoot() {
        return core.getRoot();
    }

    @Override
    public CaoActionList getActions() {
        return core.getActions();
    }

    @Override
    public String getName() {
        return core.getName();
    }

    @Override
    public CaoList executeQuery(String space, String query) throws MException {
        return core.executeQuery(space, query);
    }

    @Override
    public <T> CaoList executeQuery(String space, AQuery<T> query) throws MException {
        return core.executeQuery(space, query);
    }

    @Override
    public CaoAction getAction(String name) {
        return core.getAction(name);
    }

    @Override
    public <T extends CaoAspect> CaoAspectFactory<T> getAspectFactory(Class<T> ifc) {
        return core.getAspectFactory(ifc);
    }

    @Override
    protected void closeConnection() throws Exception {
        core.closeConnection();
    }
}
