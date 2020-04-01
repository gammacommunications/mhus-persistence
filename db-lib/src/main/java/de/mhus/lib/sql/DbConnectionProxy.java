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

import de.mhus.lib.core.M;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.base.service.UniqueId;
import de.mhus.lib.core.cfg.CfgBoolean;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.core.parser.Parser;
import de.mhus.lib.errors.MException;

/**
 * The class capsulate the real connection to bring it back into the pool if the connection in no
 * more needed - closed or cleanup by the gc.
 *
 * @author mikehummel
 */
public class DbConnectionProxy extends MObject implements DbConnection {

    private static final CfgBoolean CFG_TRACE_CALLER =
            new CfgBoolean(DbConnection.class, "traceCallers", false);

    private DbConnection instance;
    private long id = M.l(UniqueId.class).nextUniqueId();
    //	private StackTraceElement[] createStackTrace;
    private DbPool pool;

    public DbConnectionProxy(DbPool pool, DbConnection instance) {
        if (CFG_TRACE_CALLER.value()) {
            this.pool = pool;
            pool.getStackTraces().put(MSystem.getObjectId(this), new ConnectionTrace(this));
            //			instance.setUsedTrace(createStackTrace);
        }
        this.instance = instance;
        log().t(id, "created", instance.getInstanceId());
    }

    @Override
    public void commit() throws Exception {

        instance.commit();
    }

    @Override
    public boolean isReadOnly() throws Exception {
        return instance.isReadOnly();
    }

    @Override
    public void rollback() throws Exception {
        instance.rollback();
    }

    @Override
    public DbStatement getStatement(String name) throws MException {
        return instance.getStatement(name);
    }

    @Override
    public boolean isClosed() {
        if (instance == null) return true;
        return instance.isClosed();
    }

    @Override
    public boolean isUsed() {
        return instance.isUsed();
    }

    @Override
    public void setUsed(boolean used) {
        if (instance == null) return;
        instance.setUsed(used);
        if (!used) instance = null; // invalidate this proxy
    }

    @Override
    public void close() {
        if (instance == null) return;
        log().t(id, "close", instance.getInstanceId());
        setUsed(false); // close of the proxy will free the connection
        if (CFG_TRACE_CALLER.value()) pool.getStackTraces().remove(MSystem.getObjectId(this));
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void finalize() throws Throwable {
        log().t(id, "finalized", instance.getInstanceId());
        if (instance != null) {
            log().i(id, "final closed", instance.getInstanceId());
            ConnectionTrace trace = pool.getStackTraces().get(MSystem.getObjectId(this));
            if (trace != null) trace.log(log());
            setUsed(false);
        }
        if (CFG_TRACE_CALLER.value()) pool.getStackTraces().remove(MSystem.getObjectId(this));
        super.finalize();
    }

    @Override
    public DbStatement createStatement(String sql, String language) throws MException {
        return instance.createStatement(sql, language);
    }

    @Override
    public long getInstanceId() {
        return id;
    }

    @Override
    public Parser createQueryCompiler(String language) throws MException {
        return instance.createQueryCompiler(language);
    }

    @Override
    public DbConnection instance() {
        return instance;
    }

    @Override
    public DbStatement createStatement(DbPrepared dbPrepared) {
        return instance.createStatement(dbPrepared);
    }

    @Override
    public String getDefaultLanguage() {
        return instance.getDefaultLanguage();
    }

    @Override
    public String[] getLanguages() {
        return instance.getLanguages();
    }

    @Override
    public DbStatement createStatement(String sql) throws MException {
        return instance.createStatement(sql);
    }
}
