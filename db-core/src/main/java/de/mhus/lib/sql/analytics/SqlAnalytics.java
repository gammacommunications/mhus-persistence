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

import de.mhus.lib.core.logging.Log;

public class SqlAnalytics {

    private static Log log = Log.getLog(SqlAnalytics.class);
    private static SqlAnalyzer analyzer = null;

    public static void setAnalyzer(SqlAnalyzer analyzer_) {
        try {
            if (analyzer != null) analyzer.stop();
            analyzer = analyzer_;
            if (analyzer != null) analyzer.start();
        } catch (Throwable t) {
            log.e(t);
            analyzer = null;
        }
    }

    public static SqlAnalyzer getAnalyzer() {
        return analyzer;
    }

    public static void trace(
            long connectionId, String original, String query, long start, Throwable t) {
        try {
            long delta = System.currentTimeMillis() - start;
            if (analyzer != null) analyzer.doAnalyze(connectionId, original, query, delta, t);
        } catch (Throwable t2) {
            log.e(t2);
        }
    }
}
