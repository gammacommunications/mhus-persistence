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
import de.mhus.lib.cao.CaoDriver;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.errors.AccessDeniedException;

public class DriverDataSource implements CaoDataSource {

    private CaoDriver driver;
    private String uri;
    private String credentials;
    private String type;
    private String name;
    private boolean protectCore = true;

    @Override
    public String getType() {
        return type;
    }

    @Override
    public CaoConnection getConnection() throws Exception {
        CaoCore core = driver.connect(uri, credentials);
        doPrepare(core);
        return core.getConnection();
    }

    @Override
    public CaoCore getCore() throws Exception {
        if (protectCore) throw new AccessDeniedException("core is protected", name);
        CaoCore core = driver.connect(uri, credentials);
        doPrepare(core);
        return core;
    }

    protected void doPrepare(CaoCore core) {
        // TODO Auto-generated method stub

    }

    public CaoDriver getDriver() {
        return driver;
    }

    public void setDriver(CaoDriver driver) {
        this.driver = driver;
        this.type = driver.getClass().getCanonicalName();
    }

    public String getCredentials() {
        return credentials;
    }

    public void setCredentials(String credentials) {
        this.credentials = credentials;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return MSystem.toString(this, driver, uri);
    }

    public boolean isProtectCore() {
        return protectCore;
    }

    public void setProtectCore(boolean protectCore) {
        this.protectCore = protectCore;
    }
}
