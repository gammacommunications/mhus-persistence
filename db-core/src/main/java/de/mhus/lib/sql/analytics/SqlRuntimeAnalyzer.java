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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import de.mhus.lib.core.config.IConfig;

public class SqlRuntimeAnalyzer extends SqlRuntimeWarning {

    protected HashMap<String, Container> list = new HashMap<>();
    protected long minRuntime = 3;

    @Override
    public void doAnalyze(
            long connectionId, String original, String query, long delta, Throwable t) {
        super.doAnalyze(connectionId, original, query, delta, t);
        if (t != null) return;

        if (delta < minRuntime) return;

        // prepare original - replace $UUID$ entries with $uuid$ static content
        // do not use uuid any more
        //		if (original.indexOf('$') >= 0) {
        //			StringBuilder s = new StringBuilder();
        //			for (String part : original.split("\\$")) {
        //				if (s.length() != 0)
        //					s.append('$');
        //				if (MValidator.isUUID(part))
        //					s.append("uuid");
        //				else
        //					s.append(part);
        //			}
        //			original = s.toString();
        //		}

        synchronized (this) {
            Container container = list.get(original);
            if (container == null) {
                container = new Container(original);
                list.put(original, container);
            }
            container.add(delta);
        }
    }

    public Collection<Container> getData() {
        synchronized (this) {
            return Collections.unmodifiableCollection(list.values());
        }
    }

    @Override
    public void doConfigure(IConfig config) {
        minRuntime = config.getLong("minRuntime", minRuntime);
        super.doConfigure(config);
    }

    public static class Container {

        private String sql;
        private int cnt;
        private long runtime;

        public Container(String original) {
            sql = original;
        }

        public void add(long delta) {
            cnt++;
            runtime += delta;
        }

        public String getSql() {
            return sql;
        }

        public int getCnt() {
            return cnt;
        }

        public long getRuntime() {
            return runtime;
        }
    }
}
