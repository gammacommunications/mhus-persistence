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
package de.mhus.db.karaf.xdb.cmd;

import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Service;

import de.mhus.db.karaf.xdb.adb.XdbKarafApi;
import de.mhus.db.karaf.xdb.adb.XdbKarafUtil;
import de.mhus.db.osgi.api.xdb.XdbApi;
import de.mhus.db.osgi.api.xdb.XdbUtil;
import de.mhus.lib.basics.RC;
import de.mhus.lib.core.M;
import de.mhus.lib.core.util.MUri;
import de.mhus.lib.errors.MException;
import de.mhus.osgi.api.karaf.AbstractCmd;

@Command(scope = "xdb", name = "use", description = "Show or select the default api")
@Service
public class CmdUse extends AbstractCmd {

    @Argument(
            index = 0,
            name = "cmd",
            required = false,
            description =
                    "Command: save - save global settings, load - load global settings, set <uri>,apis,services",
            multiValued = false)
    String cmd = null;

    @Argument(
            index = 1,
            name = "uri",
            required = false,
            description = "Uri to the current used service, e.g. xdb:api/service[/datasource]",
            multiValued = false)
    String uriName = null;

    @Option(name = "-a", description = "New api Name", required = false)
    String apiName = null;

    @Option(name = "-s", description = "Service Name", required = false)
    String serviceName = null;

    @Option(name = "-d", description = "Datasource Name", required = false)
    String dsName = null;

    @Option(name = "-g", description = "Set Global", required = false)
    boolean global = false;

    @Override
    public Object execute2() throws Exception {

        if ("load".equals(cmd)) {
            M.l(XdbKarafApi.class).load();
        }

        if ("set".equals(cmd) && uriName != null) {
            MUri uri = MUri.toUri(uriName);
            if (!"xdb".equals(uri.getScheme())) throw new MException(RC.STATUS.ERROR, "scheme is not xdb");
            apiName = uri.getPathParts()[0];
            if (uri.getPathParts().length > 1) serviceName = uri.getPathParts()[1];
            if (uri.getPathParts().length > 2) dsName = uri.getPathParts()[2];
        }

        if (global) {
            if (apiName != null) {
                XdbUtil.getApi(apiName);
                M.l(XdbKarafApi.class).setApi(apiName);
            }

            if (serviceName != null) {
                XdbApi a = XdbUtil.getApi(M.l(XdbKarafApi.class).getApi());
                a.getService(serviceName);
                M.l(XdbKarafApi.class).setService(serviceName);
            }

            if (dsName != null) {
                M.l(XdbKarafApi.class).setDatasource(dsName); // check?
            }
        }

        if (apiName != null || serviceName != null || dsName != null)
            XdbKarafUtil.setSessionUse(
                    getSession(),
                    XdbKarafUtil.getApiName(getSession(), apiName),
                    XdbKarafUtil.getServiceName(getSession(), serviceName),
                    XdbKarafUtil.getDatasourceName(getSession(), dsName));

        System.out.println(
                "Global : xdb:"
                        + encode(M.l(XdbKarafApi.class).getApi())
                        + "/"
                        + encode(M.l(XdbKarafApi.class).getService())
                        + (M.l(XdbKarafApi.class).getDatasource() != null
                                ? "/" + encode(M.l(XdbKarafApi.class).getDatasource())
                                : ""));

        System.out.println(
                "Session: xdb:"
                        + encode(XdbKarafUtil.getApiName(getSession(), null))
                        + "/"
                        + encode(XdbKarafUtil.getServiceName(getSession(), null))
                        + (XdbKarafUtil.getDatasourceName(getSession(), null) != null
                                ? "/" + encode(XdbKarafUtil.getDatasourceName(getSession(), null))
                                : ""));

        if ("apis".equals(cmd))
            for (String n : XdbUtil.getApis()) System.out.println("Available Api: " + n);

        if ("services".equals(cmd)) {
            String an = XdbKarafUtil.getApiName(getSession(), null);
            XdbApi a = XdbUtil.getApi(an);
            System.out.println("Services in " + an + ":");
            for (String n : a.getServiceNames()) System.out.println("  " + n);
        }

        if ("save".equals(cmd)) {
            M.l(XdbKarafApi.class).save();
            System.out.println("Written global settings!");
        }

        return null;
    }

    private String encode(String in) {
        if (in == null) return null;
        return MUri.encode(in).replaceAll("/", "%2F");
    }
}
