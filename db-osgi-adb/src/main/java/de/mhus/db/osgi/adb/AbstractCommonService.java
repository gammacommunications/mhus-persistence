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
package de.mhus.db.osgi.adb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import de.mhus.db.osgi.api.adb.Reference;
import de.mhus.db.osgi.api.adb.Reference.TYPE;
import de.mhus.db.osgi.api.adb.ReferenceCollector;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.basics.UuidIdentificable;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.cfg.CfgBoolean;
import de.mhus.lib.core.cfg.CfgInt;
import de.mhus.lib.core.cfg.CfgString;
import de.mhus.lib.core.logging.Log.LEVEL;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DataSourceProvider;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DefaultDbPool;
import de.mhus.lib.sql.PseudoDbPool;
import de.mhus.osgi.api.MOsgi;
import de.mhus.osgi.api.aaa.ContextCachedItem;
import de.mhus.osgi.api.util.DataSourceUtil;

// @Component(service = AdbService.class, immediate = true)
public abstract class AbstractCommonService extends AbstractAdbService {

    private static HashMap<String, AbstractCommonService> instances = new HashMap<>();

    private ServiceTracker<CommonDbConsumer, CommonDbConsumer> tracker;
    private TreeMap<String, CommonDbConsumer> schemaList = new TreeMap<>();

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
            new CfgBoolean(AbstractCommonService.class, SERVICE_NAME + "@usePseudoPool", false);
    private final CfgBoolean CFG_ENABLED =
            new CfgBoolean(AbstractCommonService.class, SERVICE_NAME + "@enabled", true);
    private final CfgInt CFG_INIT_RETRY_SEC =
            new CfgInt(
                    AbstractCommonService.class,
                    SERVICE_NAME + "@initRetrySec",
                    1); // XXX should be 10 to 30 by default and listen for events

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

    protected abstract String getCommonServiceName();

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

    public CommonDbConsumer getConsumer(String type) throws MException {
        if (type == null) throw new MException("type is null");
        CommonDbConsumer ret = schemaList.get(type);
        if (ret == null) throw new MException("Access Controller not found", type);
        return ret;
    }

    protected boolean canRead(Object obj) throws MException {
        if (obj == null) return false;

        // XXX        Boolean item = ((AaaContextImpl) c).getCached("ace_read|" + obj.getId());
        //        if (item != null) return item;

        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        ContextCachedItem ret = new ContextCachedItem();
        ret.bool = controller.canRead(obj);
        //        ((AaaContextImpl) c)
        //                .setCached("ace_read|" + obj.getId(), MPeriod.MINUTE_IN_MILLISECOUNDS * 5,
        // ret);
        return ret.bool;
    }

    protected boolean canUpdate(Object obj) throws MException {
        if (obj == null) return false;

        //        Boolean item = ((AaaContextImpl) c).getCached("ace_update|" + obj.getId());
        //        if (item != null) return item;

        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        ContextCachedItem ret = new ContextCachedItem();
        ret.bool = controller.canUpdate(obj);
        //        ((AaaContextImpl) c)
        //               .setCached("ace_update|" + obj.getId(), MPeriod.MINUTE_IN_MILLISECOUNDS *
        // 5, ret);
        return ret.bool;
    }

    protected boolean canDelete(Object obj) throws MException {
        if (obj == null) return false;

        //        Boolean item = ((AaaContextImpl) c).getCached("ace_delete" + "|" + obj.getId());
        //        if (item != null) return item;

        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        ContextCachedItem ret = new ContextCachedItem();
        ret.bool = controller.canDelete(obj);
        //        ((AaaContextImpl) c)
        //                .setCached("ace_delete|" + obj.getId(), MPeriod.MINUTE_IN_MILLISECOUNDS *
        // 5, ret);
        return ret.bool;
    }

    protected boolean canCreate(Object obj) throws MException {
        if (obj == null) return false;
        //        Boolean item = ((AaaContextImpl) c).getCached("ace_create" + "|" + obj.getId());
        //        if (item != null) return item;

        CommonDbConsumer controller = getConsumer(obj.getClass().getCanonicalName());
        if (controller == null) return false;

        ContextCachedItem ret = new ContextCachedItem();
        ret.bool = controller.canCreate(obj);
        //        ((AaaContextImpl) c)
        //                .setCached("ace_create|" + obj.getId(), MPeriod.MINUTE_IN_MILLISECOUNDS *
        // 5, ret);
        return ret.bool;
    }

    @SuppressWarnings("unchecked")
    public <T extends Object> T getObject(String type, UUID id) throws MException {
        CommonDbConsumer controller = getConsumer(type);
        if (controller == null) return null;
        return (T) controller.getObject(type, id);
    }

    @SuppressWarnings("unchecked")
    public <T extends Object> T getObject(String type, String id) throws MException {
        CommonDbConsumer controller = getConsumer(type);
        if (controller == null) return null;
        return (T) controller.getObject(type, id);
    }

    protected void onDelete(Object object) {

        if (object == null) return;

        ReferenceCollector collector =
                new ReferenceCollector() {
                    LinkedList<UUID> list = new LinkedList<UUID>();

                    @Override
                    public void foundReference(Reference<?> ref) {
                        if (ref.getType() == TYPE.CHILD) {
                            if (ref.getObject() == null) return;
                            // be sure not cause an infinity loop, a object should only be deleted
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

        collectRefereces(object, collector);
    }

    protected void doDelete(Reference<?> ref) throws MException {
        log().d("start delete", ref.getObject(), ref.getType());
        onDelete(ref.getObject());
        log().d("delete", ref);
        getManager().delete(ref.getObject());
    }

    public void collectRefereces(Object object, ReferenceCollector collector) {

        if (object == null) return;

        HashSet<CommonDbConsumer> distinct = new HashSet<CommonDbConsumer>();
        synchronized (schemaList) {
            distinct.addAll(schemaList.values());
        }

        for (CommonDbConsumer service : distinct)
            try {
                service.collectReferences(object, collector);
            } catch (Throwable t) {
                log().w(service.getClass(), object.getClass(), t);
            }
    }

    public <T extends Object> T getObject(Class<T> type, UUID id) throws MException {
        return getObject(type.getCanonicalName(), id);
    }
}
