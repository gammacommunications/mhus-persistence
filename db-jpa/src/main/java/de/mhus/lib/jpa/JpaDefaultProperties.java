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
package de.mhus.lib.jpa;

import java.util.Properties;

import de.mhus.lib.core.config.IConfig;

public class JpaDefaultProperties extends JpaProperties {

    private static final long serialVersionUID = 1L;

    public JpaDefaultProperties() {
        super();
    }

    public JpaDefaultProperties(IConfig config) {
        super(config);
    }

    public JpaDefaultProperties(Properties arg0) {
        super(arg0);
    }

    public void configureMysql(String url, String user, String password) {
        setProperty("openjpa.ConnectionURL", url);
        setProperty("openjpa.ConnectionDriverName", "com.mysql.jdbc.Driver");
        setProperty("openjpa.ConnectionUserName", user);
        setProperty("openjpa.ConnectionPassword", password);
    }

    public void configureHsqlMemory() {
        setProperty("openjpa.ConnectionDriverName", "org.hsqldb.jdbcDriver");
        setProperty("openjpa.ConnectionURL", "jdbc:hsqldb:mem:aname");
        setProperty("openjpa.ConnectionUserName", "sa");
        setProperty("openjpa.ConnectionPassword", "");
    }

    public void enableSqlTrace() {
        setProperty("openjpa.Log", "SQL=TRACE");
    }
}
