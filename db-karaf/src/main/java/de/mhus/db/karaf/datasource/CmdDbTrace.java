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
package de.mhus.db.karaf.datasource;

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.osgi.framework.BundleContext;

import de.mhus.osgi.api.karaf.AbstractCmd;
import de.mhus.osgi.api.util.DataSourceUtil;

@Command(scope = "jdbc", name = "dbtrace", description = "Modify DB Trace")
@Service
public class CmdDbTrace extends AbstractCmd {

    @Argument(
            index = 0,
            name = "source",
            required = true,
            description = "Source Datasource",
            multiValued = false)
    String source;

    @Argument(
            index = 1,
            name = "option",
            required = true,
            description = "enable / disable",
            multiValued = false)
    String option;

    @Reference private BundleContext context;

    @Override
    public Object execute2() throws Exception {

        DataSource ds = DataSourceUtil.getDataSource(source);
        Connection con = ds.getConnection();
        if (!(con instanceof TracedConnection)) {
            System.out.println("The source is not a trace datasource");
            return null;
        }

        TraceDataSource tds = (TraceDataSource) ((TracedConnection) con).getDataSource();

        if (option.equals("enable")) {
            tds.setTrace(true);
        } else if (option.equals("disable")) {
            tds.setTrace(false);
        } else if (option.equals("log")) {
            tds.setTraceFile("");
        } else if (option.startsWith("file:")) {
            tds.setTraceFile(option.substring(5));
        }
        System.out.println(
                "Datasource "
                        + source
                        + " Trace: "
                        + tds.isTrace()
                        + " File: "
                        + tds.getTraceFile());

        return null;
    }
}
