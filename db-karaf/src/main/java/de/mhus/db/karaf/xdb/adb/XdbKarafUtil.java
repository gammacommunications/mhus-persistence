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

import org.apache.karaf.shell.api.console.Session;

import de.mhus.db.osgi.api.xdb.XdbApi;
import de.mhus.db.osgi.api.xdb.XdbUtil;
import de.mhus.lib.core.M;
import de.mhus.lib.core.MString;
import de.mhus.lib.errors.NotFoundException;
import de.mhus.lib.xdb.XdbService;
import de.mhus.lib.xdb.XdbType;

public class XdbKarafUtil {

    public static String getApiName(Session session, String apiName) {
        if (MString.isSet(apiName)) return apiName;
        apiName = (String) session.get("xdb_use_api");
        if (MString.isSet(apiName)) return apiName;
        return M.l(XdbKarafApi.class).getApi();
    }

    public static String getServiceName(Session session, String serviceName) {
        if (MString.isSet(serviceName)) return serviceName;
        serviceName = (String) session.get("xdb_use_service");
        if (MString.isSet(serviceName)) return serviceName;
        return M.l(XdbKarafApi.class).getService();
    }

    public static String getDatasourceName(Session session, String dsName) {
        if (MString.isSet(dsName)) return dsName;
        dsName = (String) session.get("xdb_use_datasource");
        if (MString.isSet(dsName)) return dsName;
        return M.l(XdbKarafApi.class).getDatasource();
    }

    public static void setSessionUse(
            Session session, String apiName, String serviceName, String dsName) {
        if (apiName != null) session.put("xdb_use_api", apiName);
        if (serviceName != null) session.put("xdb_use_service", serviceName);
        if (dsName != null) session.put("xdb_use_datasource", dsName);
    }

    public static XdbType<?> getType(String apiName, String serviceName, String typeName) throws NotFoundException {
        XdbApi api = XdbUtil.getApi(apiName);
        XdbService service = api.getService(serviceName);
        return service.getType(typeName);
    }
    
}
