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
package de.mhus.lib.sql.analytics;

import java.io.File;
import java.io.PrintStream;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import de.mhus.lib.core.MApi;
import de.mhus.lib.core.MDate;

public class SqlRuntimeWriter extends SqlRuntimeAnalyzer {

    private Timer timer;
    private File file;

    @Override
    public void start() {
        file =
                MApi.getFile(
                        MApi.SCOPE.LOG,
                        getClass().getCanonicalName()
                                + "_"
                                + MDate.toFileFormat(new Date())
                                + ".csv");
        timer = new Timer(true);
        timer.schedule(
                new TimerTask() {

                    @Override
                    public void run() {
                        doSave();
                    }
                },
                60000,
                60000);
    }

    protected void doSave() {
        try {
            PrintStream ps = new PrintStream(file);
            synchronized (this) {
                for (Container container : list.values()) {
                    ps.print(container.getSql());
                    ps.print(";");
                    ps.print(container.getCnt());
                    ps.print(";");
                    ps.println(container.getRuntime());
                }
                ps.flush();
                ps.close();
            }
        } catch (Throwable t) {
            log().e(file, t);
        }
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }
}
