/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.db.osgi.adb;

import javax.sql.DataSource;

import de.mhus.db.osgi.api.adb.AdbService;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbManagerJdbc;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.core.MActivator;
import de.mhus.lib.core.MLog;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.activator.DefaultActivator;
import de.mhus.lib.core.node.INode;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DataSourceProvider;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DefaultDbPool;
import de.mhus.lib.sql.Dialect;
import de.mhus.osgi.api.util.DataSourceUtil;
// @Component(provide=DbManagerService.class,name="...",immediate=true)
//
public abstract class AbstractAdbService extends MLog implements AdbService {

    protected String dataSourceName;
    protected String dataSourceRoName;
    private DbManager manager;

    //	protected abstract void doInitialize() throws Exception;

    /**
     * Call this function in the doActivate() after you set the context and dataSourceName
     * attribute.
     *
     * @throws Exception
     */
    protected void doOpen(boolean clean) throws MException {
        if (manager != null) return;
        doInitialize();

        if (getDataSource() == null) return;

        manager = doCreateDbManager(clean);

        doPostOpen();
    }

    protected void doPostOpen() throws MException {}

    @Override
    public void doClose() {
        if (manager == null) return;
        manager.getPool().close();
        manager = null;
    }

    protected DbManager doCreateDbManager(boolean clean) throws MException {

        DbPool pool = doCreateDataPool();
        DbPool poolRo = doCreateRoDataPool();
        DbSchema schema = doCreateSchema();
        return new DbManagerJdbc(dataSourceName, pool, poolRo, schema, clean); // TODO configurable
    }

    protected abstract DbSchema doCreateSchema();

    protected DbPool doCreateDataPool() {
        return new DefaultDbPool(
                new DataSourceProvider(
                        getDataSource(), doCreateDialect(), doCreateConfig(), doCreateActivator()));
    }

    protected DbPool doCreateRoDataPool() {
    	if (MString.equals(dataSourceName, dataSourceRoName))
    		return null;
    	DataSource ds = getDataSourceRo();
    	if (ds == null) return null;
        return new DefaultDbPool(
                new DataSourceProvider(
                        ds, doCreateDialect(), doCreateConfig(), doCreateActivator()));
    }
    
    protected MActivator doCreateActivator() {
        try {
            return new DefaultActivator(null, getClass().getClassLoader());
        } catch (MException e) {
            log().e(e);
        }
        return null;
    }

    protected INode doCreateConfig() {
        return null;
    }

    protected Dialect doCreateDialect() {
        return null;
    }

    protected DataSource getDataSource() {
        DataSource ds = DataSourceUtil.getDataSource(dataSourceName);
        if (ds == null) log().w("DataSource is unknown", dataSourceName);
        return ds;
    }

    protected DataSource getDataSourceRo() {
    	if (MString.equals(dataSourceName, dataSourceRoName))
    		return getDataSource();
    	
        DataSource ds = DataSourceUtil.getDataSource(dataSourceRoName);
        if (ds == null) {
        	log().w("DataSourceRo is unknown", dataSourceRoName);
            ds = DataSourceUtil.getDataSource(dataSourceName); // try RW datasource
            if (ds != null)
            	log().w("DataSourceRo fallback to RW");
        }
        return ds;
    }
    
    @Override
    public void updateManager(boolean clean) throws MException {
        doClose();
        //		if (!isConnected()) {
        if (getDataSource() == null) return;
        doOpen(clean);
        //			return;
        //		}
        //		((DataSourceProvider)manager.getPool().getProvider()).setDataSource(getDataSource());
    }

    @Override
    public DbManager getManager() {
        try {
            doOpen(false);
        } catch (Exception e) {
            log().w(e);
        }
        return manager;
    }

    @Override
    public boolean isConnected() {
        return manager != null;
    }

    @Override
    public String getDataSourceName() {
        return dataSourceName;
    }

    @Override
    public String getDataSourceRoName() {
        return dataSourceRoName;
    }
    
    @Override
    public void setDataSourceName(String dataSourceName) {
        if (MSystem.equals(this.dataSourceName, dataSourceName)) return;
        this.dataSourceName = dataSourceName;
        try {
            updateManager(false);
        } catch (Exception e) {
        }
    }

    @Override
    public void setDataSourceRoName(String dataSourceName) {
        if (MSystem.equals(this.dataSourceRoName, dataSourceName)) return;
        this.dataSourceRoName = dataSourceName;
        try {
            updateManager(false);
        } catch (Exception e) {
        }
    }
    
    @Override
    public String getServiceName() {
        return getClass().getSimpleName();
    }
}
