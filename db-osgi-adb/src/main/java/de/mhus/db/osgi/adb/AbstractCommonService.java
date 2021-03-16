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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import javax.sql.DataSource;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import de.mhus.db.osgi.api.adb.CommonDbConsumer;
import de.mhus.db.osgi.api.adb.CommonService;
import de.mhus.db.osgi.api.adb.Reference;
import de.mhus.db.osgi.api.adb.Reference.TYPE;
import de.mhus.db.osgi.api.adb.ReferenceCollector;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.basics.UuidIdentificable;
import de.mhus.lib.core.M;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.aaa.Aaa;
import de.mhus.lib.core.cache.ICache;
import de.mhus.lib.core.cache.CacheConfig;
import de.mhus.lib.core.cache.ICacheService;
import de.mhus.lib.core.cfg.CfgBoolean;
import de.mhus.lib.core.cfg.CfgInt;
import de.mhus.lib.core.cfg.CfgLong;
import de.mhus.lib.core.cfg.CfgString;
import de.mhus.lib.core.logging.Log.LEVEL;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DataSourceProvider;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DefaultDbPool;
import de.mhus.lib.sql.PseudoDbPool;
import de.mhus.osgi.api.MOsgi;
import de.mhus.osgi.api.util.DataSourceUtil;

// @Component(service = AdbService.class, immediate = true)
public abstract class AbstractCommonService extends AbstractAdbService implements CommonService {

    private static HashMap<String, AbstractCommonService> instances = new HashMap<>(); //TODO should use a service for it

    private ServiceTracker<CommonDbConsumer, CommonDbConsumer> tracker;
    private TreeMap<String, CommonDbConsumer> schemaList = new TreeMap<>();
    private TreeMap<String, CommonDbConsumer> objectTypes = new TreeMap<>();

    private BundleContext context;

    enum STATUS {
        NONE,
        ACTIVATED,
        STARTED,
        CLOSED
    }

    private STATUS status = STATUS.NONE;

    // static service name
    protected final String SERVICE_NAME = getCommonServiceName();

    private final CfgString CFG_DATASOURCE =
            new CfgString(
                            AbstractCommonService.class,
                            SERVICE_NAME + "@dataSourceName",
                            "adb_common")
                    .updateAction(
                            s -> {
                                setDataSourceName(s);
                            });
    private final CfgBoolean CFG_USE_PSEUDO =
            new CfgBoolean(AbstractCommonService.class, SERVICE_NAME + "@pseudoPoolEnabled", false);
    private final CfgBoolean CFG_ENABLED =
            new CfgBoolean(AbstractCommonService.class, SERVICE_NAME + "@enabled", true);
    private final CfgInt CFG_INIT_RETRY_SEC =
            new CfgInt(
                    AbstractCommonService.class,
                    SERVICE_NAME + "@initRetrySec",
                    1); // XXX should be 10 to 30 by default and listen for events

    private CfgBoolean CFG_USE_ACCESS_CACHE_API = new CfgBoolean(AbstractCommonService.class, SERVICE_NAME + "@accessCacheEnabled", true);

    private final CfgLong CFG_ACCESS_CACHE_TTL =
            new CfgLong(
                    AbstractCommonService.class,
                    SERVICE_NAME + "@accessCacheTTL",
                    MPeriod.MINUTE_IN_MILLISECOUNDS * 15); 

    private final CfgInt CFG_ACCESS_CACHE_SIZE =
            new CfgInt(
                    AbstractCommonService.class,
                    SERVICE_NAME + "@accessCacheSize",
                    1000000); 

    private ICache<String, Boolean> accessCache;


    public static AbstractCommonService instance(String name) {
        return instances.get(name);
    }

    public static String[] instances() {
        return instances.keySet().toArray(new String[0]);
    }

    @Activate
    public void doActivate(ComponentContext ctx) {

        dataSourceName = CFG_DATASOURCE.value();
        status = STATUS.ACTIVATED;
        //		new de.mhus.lib.adb.util.Property();
        context = ctx.getBundleContext();

        if (context == null) return;

        instances.put(getCommonServiceName(), this);
        if (!CFG_ENABLED.value()) {
            log().i("not enabled");
            return;
        }
        MOsgi.runAfterActivation(ctx, this::doStart);
    }

    public void doStart(ComponentContext ctx) {
        boolean once = true;
        while (true) {
            try {
                AbstractCommonService.this.updateManager(false);
            } catch (Throwable e1) {
                log().e(e1);
            }
            if (status == STATUS.STARTED) return;
            try {
                DataSource ds =
                        DataSourceUtil.getDataSource(
                                getDataSourceName(),
                                ctx == null ? MOsgi.getBundleContext() : ctx.getBundleContext());
                if (ds != null) {
                    if (getManager() != null) {
                        log().i("Start tracker");
                        try {
                            tracker =
                                    new ServiceTracker<>(
                                            context,
                                            CommonDbConsumer.class,
                                            new MyTrackerCustomizer());
                            tracker.open();
                        } finally {
                            status = STATUS.STARTED;
                        }
                        return;
                    }
                }
            } catch (java.lang.IllegalStateException e) {
                log().e("Exit CommonAdbService start loop", e.toString());
                return;
            }

            // write once as info
            log().log(
                            once ? LEVEL.INFO : LEVEL.TRACE,
                            "Waiting for datasource",
                            getDataSourceName());
            once = false;
            MThread.sleep(CFG_INIT_RETRY_SEC.value() * 1000);
        }
    }

    @Override
    protected DataSource getDataSource() {
        DataSource ds = DataSourceUtil.getDataSource(dataSourceName, context);
        if (ds == null) log().t("DataSource is unknown", dataSourceName);
        return ds;
    }

    @Deactivate
    public void doDeactivate(ComponentContext ctx) {
        try {
            status = STATUS.CLOSED;
            //		super.doDeactivate(ctx);
            if (tracker != null) tracker.close();
            tracker = null;
            context = null;
            schemaList.clear();
            objectTypes.clear();
        } finally {
            instances.remove(getCommonServiceName());
        }
    }

    @Override
    protected abstract DbSchema doCreateSchema();

    @Override
    public abstract void doInitialize() throws MException;

    @Override
    protected DbPool doCreateDataPool() {
        if (CFG_USE_PSEUDO.value())
            return new PseudoDbPool(
                    new DataSourceProvider(
                            getDataSource(),
                            doCreateDialect(),
                            doCreateConfig(),
                            doCreateActivator()));
        else
            return new DefaultDbPool(
                    new DataSourceProvider(
                            getDataSource(),
                            doCreateDialect(),
                            doCreateConfig(),
                            doCreateActivator()));
    }

    private class MyTrackerCustomizer
            implements ServiceTrackerCustomizer<CommonDbConsumer, CommonDbConsumer> {

        @Override
        public CommonDbConsumer addingService(ServiceReference<CommonDbConsumer> reference) {

            if (!AbstractCommonService.this.acceptService(reference)) return null;

            CommonDbConsumer service = context.getService(reference);
            String name = service.getClass().getCanonicalName();
            service.doInitialize(AbstractCommonService.this.getManager());

            synchronized (schemaList) {
                schemaList.put(name, service);
                List<Class<? extends Object>> list = new ArrayList<>();
                service.registerObjectTypes(list);
                list.forEach(v -> objectTypes.put(v.getCanonicalName(), service) );
                updateManager();
            }

            if (AbstractCommonService.this.getManager() != null) {
                servicePostInitialize(service, name);
            }
            return service;
        }

        @Override
        public void modifiedService(
                ServiceReference<CommonDbConsumer> reference, CommonDbConsumer service) {

            if (!AbstractCommonService.this.acceptService(reference)) return;

            synchronized (schemaList) {
                updateManager();
            }
        }

        @Override
        public void removedService(
                ServiceReference<CommonDbConsumer> reference, CommonDbConsumer service) {

            if (!AbstractCommonService.this.acceptService(reference)) return;

            String name = service.getClass().getCanonicalName();
            service.doDestroy();
            synchronized (schemaList) {
                schemaList.remove(name);
                List<Class<? extends Object>> list = new ArrayList<>();
                service.registerObjectTypes(list);
                HashSet<String> set = new HashSet<>();
                list.forEach(v -> set.add(v.getCanonicalName()));
                objectTypes.keySet().removeIf(k -> set.contains(k) );
                updateManager();
            }
        }
    }

    protected void updateManager() {
        try {
            DbManager m = getManager();
            if (m != null) m.reconnect();
        } catch (Exception e) {
            log().e(e);
        }
    }

    protected boolean acceptService(ServiceReference<CommonDbConsumer> reference) {
        String name = getCommonServiceName();
        Object refName = reference.getProperty("commonService");
        return name.equals(refName);
    }

    @Override
    public abstract String getCommonServiceName();

    protected void servicePostInitialize(CommonDbConsumer service, String name) {
        MThread.asynchron(
                new Runnable() {
                    @Override
                    public void run() {
                        // wait for STARTED
                        while (status == STATUS.ACTIVATED
                                || AbstractCommonService.this.getManager().getPool() == null) {
                            log().d("Wait for start", service);
                            MThread.sleep(250);
                        }
                        // already open
                        log().d("addingService", "doPostInitialize", name);
                        try {
                            service.doPostInitialize(AbstractCommonService.this.getManager());
                        } catch (Throwable t) {
                            log().w(name, t);
                        }
                    }
                });
    }

    public CommonDbConsumer[] getConsumer() {
        synchronized (schemaList) {
            return schemaList.values().toArray(new CommonDbConsumer[schemaList.size()]);
        }
    }

    @Override
    public final String getServiceName() {
        return SERVICE_NAME;
    };

    @Override
    protected void doPostOpen() throws MException {
        synchronized (schemaList) {
            schemaList.forEach(
                    (name, service) -> {
                        log().d("doPostOpen", "doPostInitialize", name);
                        servicePostInitialize(service, name);
                    });
        }
    }

    public STATUS getStatus() {
        return status;
    }

    // ----
    // Access

    @Override
    public CommonDbConsumer getConsumer(String type) throws MException {
        if (type == null) throw new MException("type is null");
        CommonDbConsumer ret = objectTypes.get(type);
        if (ret == null) {
            log().t("Access Controller not found",type,objectTypes);
            throw new MException("Access Controller not found", type);
        }
        return ret;
    }

    @Override
    public boolean canRead(Object obj) throws MException {
        if (obj == null) return false;

        String clazz = obj.getClass().getCanonicalName();
        if (obj instanceof UuidIdentificable) {
            Boolean cached = getCachedAccess("read", clazz, ((UuidIdentificable)obj).getId());
            if (cached != null) return cached;
        }
        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        boolean bool = controller.canRead(obj);
        if (obj instanceof UuidIdentificable)
            doCacheAccess("read", clazz, ((UuidIdentificable)obj).getId(), bool);
        return bool;
    }

    @Override
    public boolean canUpdate(Object obj) throws MException {
        if (obj == null) return false;

        String clazz = obj.getClass().getCanonicalName();
        if (obj instanceof UuidIdentificable) {
            Boolean cached = getCachedAccess("update", clazz, ((UuidIdentificable)obj).getId());
            if (cached != null) return cached;
        }
        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        boolean bool = controller.canUpdate(obj);
        if (obj instanceof UuidIdentificable)
            doCacheAccess("update", clazz, ((UuidIdentificable)obj).getId(), bool);
        return bool;
    }

    @Override
    public boolean canDelete(Object obj) throws MException {
        if (obj == null) return false;

        String clazz = obj.getClass().getCanonicalName();
        if (obj instanceof UuidIdentificable) {
            Boolean cached = getCachedAccess("delete", clazz, ((UuidIdentificable)obj).getId());
            if (cached != null) return cached;
        }
        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        boolean bool = controller.canDelete(obj);
        if (obj instanceof UuidIdentificable)
            doCacheAccess("delete", clazz, ((UuidIdentificable)obj).getId(), bool);
        return bool;
    }

    @Override
    public boolean canCreate(Object obj) throws MException {
        if (obj == null) return false;

        String clazz = obj.getClass().getCanonicalName();
        if (obj instanceof UuidIdentificable) {
            Boolean cached = getCachedAccess("create", clazz, null);
            if (cached != null) return cached;
        }
        CommonDbConsumer controller = getConsumer(clazz);
        if (controller == null) return false;

        boolean bool = controller.canCreate(obj);
        if (obj instanceof UuidIdentificable)
            doCacheAccess("create", clazz, null, bool);
        return bool;
    }

    protected void doCacheAccess(String action, String clazz, UUID id, boolean value) {
        initAccessCache();
        if (accessCache == null) return;
        String account = Aaa.getPrincipal();
        accessCache.put(account + ":" + action + "@" + id + ":" + clazz, value);
    }

    protected Boolean getCachedAccess(String action, String clazz, UUID id) {
        initAccessCache();
        if (accessCache == null) return null;
        String account = Aaa.getPrincipal();
        return accessCache.get(account + ":" + action + "@" + id + ":" + clazz);
    }

    protected synchronized void initAccessCache() {
        if (accessCache != null) return;
        if (!CFG_USE_ACCESS_CACHE_API.value()) return;
        try {
            ICacheService cacheService = M.l(ICacheService.class);
            accessCache =
                    cacheService.createCache(
                            this,
                            "accessCache@" + getServiceName(),
                            String.class,
                            Boolean.class,
                            new CacheConfig().setHeapSize(CFG_ACCESS_CACHE_SIZE.value()).setTTL(CFG_ACCESS_CACHE_TTL.value())
                            );
        } catch (Throwable e) {
            log().d(getServiceName(),e.toString());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Object> T getObject(String type, UUID id) throws MException {
        CommonDbConsumer controller = getConsumer(type);
        if (controller == null) return null;
        return (T) controller.getObject(type, id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Object> T getObject(String type, String id) throws MException {
        CommonDbConsumer controller = getConsumer(type);
        if (controller == null) return null;
        return (T) controller.getObject(type, id);
    }

    @Override
    public void onDelete(Object object) {

        if (object == null) return;

        ReferenceCollector collector =
                new ReferenceCollector() {
                    LinkedList<UUID> list = new LinkedList<UUID>();

                    @Override
                    public void foundReference(Reference<?> ref) {
                        if (ref.getType() == TYPE.CHILD) {
                            if (ref.getObject() == null) return;
                            // be sure not to cause an infinity loop, a object should only be deleted
                            // once ...
                            if (ref.getObject() instanceof UuidIdentificable) {
                                if (list.contains(((UuidIdentificable) ref.getObject()).getId()))
                                    return;
                                list.add(((UuidIdentificable) ref.getObject()).getId());
                            }
                            // delete the object and dependencies
                            try {
                                doDelete(ref);
                            } catch (MException e) {
                                log().w(
                                                "deletion failed",
                                                ref.getObject(),
                                                ref.getObject().getClass(),
                                                e);
                            }
                        }
                    }
                };

        collectRefereces(object, collector, CommonDbConsumer.REASON_DELETE);
    }

    protected void doDelete(Reference<?> ref) throws MException {
        log().d("start delete", ref.getObject(), ref.getType());
        onDelete(ref.getObject());
        log().d("delete", ref);
        getManager().delete(ref.getObject());
    }

    @Override
    public void collectRefereces(Object object, ReferenceCollector collector, String reason) {

        if (object == null) return;

        HashSet<CommonDbConsumer> distinct = new HashSet<CommonDbConsumer>();
        synchronized (schemaList) {
            distinct.addAll(schemaList.values());
        }

        for (CommonDbConsumer service : distinct)
            try {
                service.collectReferences(object, collector, reason);
            } catch (Throwable t) {
                log().w(service.getClass(), object.getClass(), t);
            }
    }

    @Override
    public <T extends Object> T getObject(Class<T> type, UUID id) throws MException {
        return getObject(type.getCanonicalName(), id);
    }
}
