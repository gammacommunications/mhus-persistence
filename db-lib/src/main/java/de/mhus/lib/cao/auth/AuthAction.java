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

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;

public class AuthAction extends CaoAction {

    protected CaoAction instance;
    private AuthCore con;

    public AuthAction(AuthCore con, CaoAction instance) {
        this.con = con;
        this.instance = instance;
    }

    @Override
    public String getName() {
        return instance.getName();
    }

    @Override
    public CaoConfiguration createConfiguration(CaoList list, IProperties configuration)
            throws CaoException {
        // unpack
        CaoNode parent = list.getParent();
        if (parent != null && (parent instanceof AuthNode)) parent = ((AuthNode) parent).instance;
        CaoList to = new CaoList(parent);
        for (CaoNode n : list) {
            if (n != null && (n instanceof AuthNode)) n = ((AuthNode) n).instance;
            to.add(n);
        }
        return instance.createConfiguration(to, configuration);
    }

    @Override
    public boolean canExecute(CaoConfiguration configuration) {
        if (!con.hasActionAccess(configuration, this)) return false;
        return instance.canExecute(configuration);
    }

    @Override
    public OperationResult doExecuteInternal(CaoConfiguration configuration, Monitor monitor)
            throws CaoException {

        if (!con.hasActionAccess(configuration, this))
            return new NotSuccessful(getName(), "access denied", OperationResult.ACCESS_DENIED);

        OperationResult res = instance.doExecute(configuration, monitor);

        if (res != null) {
            if (res.isResult(CaoNode.class)) {
                res.setResult(new AuthNode(con, res.getResultAs(CaoNode.class)));
            } else if (res.isResult(CaoList.class)) {
                CaoList from = res.getResultAs(CaoList.class);
                CaoNode parent = from.getParent();
                if (parent != null) parent = new AuthNode(con, parent);
                CaoList to = new CaoList(parent);
                for (CaoNode n : from) to.add(new AuthNode(con, n));
                res.setResult(to);
            }
        }
        return res;
    }
}
