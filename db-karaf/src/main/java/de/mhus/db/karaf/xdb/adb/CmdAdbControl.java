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

import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;

import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;

import de.mhus.db.osgi.adb.CommonAdbService;
import de.mhus.db.osgi.api.adb.AdbOsgiUtil;
import de.mhus.db.osgi.api.adb.AdbService;
import de.mhus.db.osgi.api.adb.CommonConsumer;
import de.mhus.lib.core.MCast;
import de.mhus.lib.core.console.ConsoleTable;
import de.mhus.lib.sql.DbPool;
import de.mhus.osgi.api.karaf.AbstractCmd;

@Command(scope = "xdb", name = "adb", description = "Control ADB specific attributes")
@Service
public class CmdAdbControl extends AbstractCmd {

    @Argument(
            index = 0,
            name = "command",
            required = true,
            description =
                    "Command:\n"
                            + " consumers - list all consumers and managed types\n"
                            + " jmx-list <serviceName> - list all pools using jmx\n"
                            + " jmx-all  - list all jmx components - debug only\n"
                            + " info     <serviceName> - info about the adb service\n"
                            + " cleanup  <serviceName> <unused also (true)>- cleanup pool\n"
                            + " datasource <name> - change datasource (be aware!)\n"
                            + " mapping  - print service mappings",
            multiValued = false)
    String cmd;

    @Argument(
            index = 1,
            name = "arguments",
            required = false,
            description = "Arguments for the command",
            multiValued = true)
    String[] args;

    @Override
    public Object execute2() throws Exception {

        if (cmd.equals("start")) {
            CommonAdbService.instance(args[0]).doStart(null);
        }
        if (cmd.equals("consumers")) {
            ConsoleTable table = new ConsoleTable(tblOpt);
            table.setHeaderValues("Consumer","Managed Types","Instance");
            for (String commonName : CommonAdbService.instances()) {
                for (CommonConsumer consumer : CommonAdbService.instance(commonName).getConsumer()) {
                    LinkedList<Class<? extends Object>> list = new LinkedList<>();
                    consumer.registerObjectTypes(list);
                    table.addRowValues(consumer.getClass().getCanonicalName(), list,commonName);
                }
            }
            table.print();
        } else
        if (cmd.equals("mapping")) {
            AdbService service = AdbOsgiUtil.getService(args[0]);

            ConsoleTable table = new ConsoleTable(tblOpt);
            table.setHeaderValues("Key", "Mapping");

            Map<String, Object> map = service.getManager().getNameMapping();
            for (String entry : new TreeSet<String>(map.keySet())) {
                table.addRowValues(entry, String.valueOf(map.get(entry)));
            }

            table.print();
        } else if (cmd.equals("datasource")) {
            AdbService service = AdbOsgiUtil.getService(args[0]);
            if (args.length > 1) service.setDataSourceName(args[1]);
            System.out.println("Datasource: " + service.getDataSourceName());
        } else if (cmd.equals("jmx-list")) {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ConsoleTable out = new ConsoleTable(tblOpt);
            out.setHeaderValues("Id", "Name", "Class", "Size", "Used");
            for (ObjectInstance instance : server.queryMBeans(null, null)) {
                if (instance.getObjectName()
                        .getCanonicalName()
                        .startsWith("de.mhus.lib.core.jmx.JmxObject:name=de.mhus.lib.sql.DbPool")) {
                    Object size = null;
                    Object usedSize = null;
                    Object id = null;
                    try {
                        id = server.getAttribute(instance.getObjectName(), "PoolId");
                        size = server.getAttribute(instance.getObjectName(), "Size");
                        usedSize = server.getAttribute(instance.getObjectName(), "UsedSize");
                    } catch (Throwable t) {
                    }
                    if (size != null)
                        out.addRowValues(
                                id,
                                instance.getObjectName().getCanonicalName(),
                                instance.getClassName(),
                                size,
                                usedSize);
                }
            }
            out.print(System.out);
        } else if (cmd.equals("info")) {

            AdbService service = AdbOsgiUtil.getService(args[0]);
            System.out.println("Pool     : " + service.getManager().getPool().getClass());
            System.out.println("Pool Size: " + service.getManager().getPool().getSize());
            System.out.println("Pool Used: " + service.getManager().getPool().getUsedSize());
            System.out.println("DataSource Name:" + service.getManager().getDataSourceName());
            System.out.println("Schema     : " + service.getManager().getSchema().getClass());
            System.out.println("Schema Name: " + service.getManager().getSchemaName());
        }
        if (cmd.equals("cleanup")) {

            AdbService service = AdbOsgiUtil.getService(args[0]);
            DbPool pool = service.getManager().getPool();
            pool.cleanup(args.length > 1 ? MCast.toboolean(args[1], false) : false);
            System.out.println("Size  : " + pool.getSize());
            System.out.println("Unused: " + pool.getUsedSize());

        } else if (cmd.equals("jmx-all")) {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            for (ObjectInstance instance : server.queryMBeans(null, null)) {
                System.out.println("MBean Found");
                System.out.println("Class Name : " + instance.getClassName());
                System.out.println("Object Name: " + instance.getObjectName());
                System.out.println("****************************************");
            }
        }

        return null;
    }
}
