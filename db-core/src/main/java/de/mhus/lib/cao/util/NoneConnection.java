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
package de.mhus.lib.cao.util;

import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoDriver;
import de.mhus.lib.cao.CaoNode;

/**
 * NoneConnection class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class NoneConnection extends CaoCore {

    /**
     * Constructor for NoneConnection.
     *
     * @param name
     * @param driver a {@link de.mhus.lib.cao.CaoDriver} object.
     */
    public NoneConnection(String name, CaoDriver driver) {
        super(name, driver);
        this.con = this;
    }

    /** {@inheritDoc} */
    @Override
    public CaoNode getResourceByPath(String name) {
        return null;
    }

    @Override
    public CaoNode getRoot() {
        return null;
    }

    @Override
    public CaoNode getResourceById(String id) {
        return null;
    }

    @Override
    protected void closeConnection() throws Exception {}
}
