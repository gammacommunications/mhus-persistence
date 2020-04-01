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

import de.mhus.lib.cao.CaoConnection;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoDataSource;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.errors.AccessDeniedException;

public class SharedDataSource implements CaoDataSource {

    private CaoCore shared;
    private String name;
    private String type;
    private boolean protectCore = true;

    public SharedDataSource() {}

    public SharedDataSource(CaoCore shared) {
        this(shared.getName(), shared, true);
    }

    public SharedDataSource(CaoCore shared, boolean isProtected) {
        this(shared.getName(), shared, isProtected);
    }

    public SharedDataSource(String name, CaoCore shared, boolean isProtected) {
        this.shared = shared;
        this.name = name;
        this.type = shared.getClass().getSimpleName();
        this.protectCore = isProtected;
        shared.setShared();
    }

    public void setConnection(CaoCore connection) {
        this.shared = connection;
        this.type = connection.getClass().getCanonicalName();
        if (name == null) name = connection.getName();
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public CaoConnection getConnection() {
        return shared.getConnection();
    }

    @Override
    public CaoCore getCore() throws Exception {
        if (protectCore) throw new AccessDeniedException("core is protected", name);
        return shared;
    }

    @Override
    public String toString() {
        return MSystem.toString(this, shared);
    }

    public boolean isProtectCore() {
        return protectCore;
    }

    public void setProtectCore(boolean protectCore) {
        this.protectCore = protectCore;
    }
}
