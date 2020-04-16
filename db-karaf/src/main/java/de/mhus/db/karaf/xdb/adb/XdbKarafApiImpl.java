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
package de.mhus.db.karaf.xdb.adb;

import java.util.Dictionary;

import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;

import de.mhus.lib.core.lang.MObject;
import de.mhus.osgi.api.services.MOsgi;

@Component
public class XdbKarafApiImpl extends MObject implements XdbKarafApi {

    private String api = "adb";
    private String service = "adb";
    private String datasource = "";
    private ConfigurationAdmin configurationAdmin;

    @Activate
    public void doActivate() {
        load();
    }

    @Modified
    public void modified(ComponentContext ctx) {
        load();
    }

    @Override
    public void load() {
        try {
            Dictionary<String, Object> prop = MOsgi.loadConfiguration(configurationAdmin, XdbKarafApiImpl.class);
            if (prop.get("api") != null)
                api = (String) prop.get("api");
            if (prop.get("service") != null)
                service = (String) prop.get("service");
            if (prop.get("datasource") != null)
                datasource = (String) prop.get("datasource");
        } catch (Throwable t) {
            log().d(t);
        }
    }

    @Override
    public void save() {
        try {
            Dictionary<String, Object> prop = MOsgi.loadConfiguration(configurationAdmin, XdbKarafApiImpl.class);
            prop.put("api", api );
            prop.put("service", service );
            prop.put("datasource", datasource );
            MOsgi.saveConfiguration(configurationAdmin, XdbKarafApiImpl.class, prop);
        } catch (Throwable t) {
            log().d(t);
        }
    }

    @Override
    public String getApi() {
        return api;
    }

    @Override
    public void setApi(String api) {
        this.api = api;
    }

    @Override
    public String getService() {
        return service;
    }

    @Override
    public void setService(String service) {
        this.service = service;
    }

    @Override
    public String getDatasource() {
        return datasource;
    }

    @Override
    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }
    
    @Reference
    public void setConfigurationAdmin(ConfigurationAdmin admin) {
        configurationAdmin = admin;
    }
    
}
