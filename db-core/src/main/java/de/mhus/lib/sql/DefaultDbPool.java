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
package de.mhus.lib.sql;

import java.util.LinkedList;
import java.util.List;

import de.mhus.lib.annotations.jmx.JmxManaged;
import de.mhus.lib.core.MActivator;
import de.mhus.lib.core.config.IConfig;

/**
 * The pool handles a bundle of connections. The connections should have the same credentials (url,
 * user access). Unused or closed connections will be freed after a pending period.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DefaultDbPool extends DbPool {

    private List<InternalDbConnection> pool = new LinkedList<InternalDbConnection>();

    /**
     * Create a new pool from central configuration. It's used the MApi configuration with the key
     * of this class.
     *
     * @throws java.lang.Exception if any.
     */
    public DefaultDbPool() throws Exception {
        super(null, null);
    }

    /**
     * Create a new pool from a configuration.
     *
     * @param config Config element or null. null will use the central MApi configuration.
     * @param activator Activator or null. null will use the central MApi Activator.
     * @throws java.lang.Exception if any.
     */
    public DefaultDbPool(IConfig config, MActivator activator) throws Exception {
        super(config, activator);
    }

    /**
     * Create a pool with the DbProvider.
     *
     * @param provider a {@link de.mhus.lib.sql.DbProvider} object.
     */
    public DefaultDbPool(DbProvider provider) {
        super(provider);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Look into the pool for an unused DbProvider. If no one find, create one.
     */
    @Override
    public DbConnection getConnection() throws Exception {
        log().t(getName(), "getConnection");
        boolean foundClosed = false;
        try {
            synchronized (pool) {
                for (InternalDbConnection con : pool) {
                    if (con.isClosed() || con.checkTimedOut()) {
                        foundClosed = true;
                    } else if (!con.isUsed()) {
                        con.setUsed(true);
                        return new DbConnectionProxy(this, con);
                    }
                }
                return createConnection();
            }
        } finally {
            if (foundClosed) cleanup(false);
        }
    }

    /**
     * Overwrite to configure new created connections before use.
     *
     * @return created connection or null if not possible
     * @throws Exception
     */
    protected DbConnection createConnection() throws Exception {
        try {
            InternalDbConnection con = getProvider().createConnection();
            if (con == null) return null;
            con.setPool(this);
            pool.add(con);
            if (tracePoolSize.value()) log().d("Create DB Connection", pool.size());
            con.setUsed(true);
            // getDialect().initializeConnection(con, this);
            return new DbConnectionProxy(this, con);
        } catch (Exception e) {
            // special behavior for e.g. mysql, retry to get a connection after gc()
            // Caused by: com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException: Too
            // many connections
            if (e.getMessage() != null && e.getMessage().indexOf("Too many connections") > -1) {
                printStackTrace();
            }
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Current pool size.
     */
    @Override
    @JmxManaged(descrition = "Current size of the pool")
    public int getSize() {
        synchronized (pool) {
            return pool.size();
        }
    }

    /** {@inheritDoc} */
    @Override
    @JmxManaged(descrition = "Current used connections in the pool")
    public int getUsedSize() {
        int cnt = 0;
        synchronized (pool) {
            for (DbConnection con : new LinkedList<DbConnection>(pool)) {
                if (con.isUsed()) cnt++;
            }
        }
        return cnt;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cleanup the connection pool. Unused or closed connections will be removed. TODO new
     * strategy to remove unused connections - not prompt, need a timeout time or minimum pool size.
     */
    @Override
    @JmxManaged(descrition = "Cleanup unused connections")
    public void cleanup(boolean unusedAlso) {
        log().t(getName(), "cleanup");
        boolean removed = false;
        synchronized (pool) {
            for (InternalDbConnection con : new LinkedList<InternalDbConnection>(pool)) {
                try {
                    con.checkTimedOut();
                    if (unusedAlso && !con.isUsed() || con.isClosed()) {
                        con.close();
                        pool.remove(con);
                        removed = true;
                    }
                } catch (Throwable t) {
                } // for secure - do not impact the thread
            }
            if (removed && tracePoolSize.value()) log().d("Pool cleanup", pool.size());
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Close the pool and all connections.
     */
    @Override
    public void close() {
        if (pool == null) return;
        log().t(getName(), "close");
        synchronized (pool) {
            for (DbConnection con : pool) {
                con.close();
            }
            pool = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    @JmxManaged(descrition = "Return the usage of the connections")
    public String dumpUsage(boolean used) {
        StringBuilder out = new StringBuilder();
        synchronized (pool) {
            for (ConnectionTrace trace : getStackTraces().values()) {
                out.append(trace.toString()).append("\n");
            }
        }
        return out.toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() {
        return pool == null;
    }
}
