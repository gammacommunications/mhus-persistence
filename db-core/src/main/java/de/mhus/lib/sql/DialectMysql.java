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
package de.mhus.lib.sql;

import java.text.SimpleDateFormat;
import java.util.Date;

import de.mhus.lib.annotations.adb.DbType;
import de.mhus.lib.core.MSql;
import de.mhus.lib.core.config.IConfig;

/**
 * This class can compare a configuration with a database table structure and can modify the
 * database structure without deleting existing tables.
 *
 * <p>TODO: on request: remove other columns TODO: views, foreign keys TODO: data !!!
 *
 * @author mikehummel
 */
public class DialectMysql extends DialectDefault {

    @Override
    public String normalizeColumnName(String columnName) {
        //		if ("key".equals(columnName))
        //			return "key_";
        //		return columnName;
        return columnName + "_"; // TODO not working at all
    }

    @Override
    public String getDbType(String type, String size) {
        String t = type.toUpperCase();

        if (t.equals(DbType.TYPE.FLOAT.name())) {
            t = "DOUBLE"; // Float is too small - use double
        } else if (t.equals(DbType.TYPE.BLOB.name())) {
            t = "LONGBLOB"; // blob is too small - use longblob
        } else return super.getDbType(t, size);

        return t;
    }

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String toSqlDate(Date date) {
        synchronized (dateFormat) {
            return "'" + dateFormat.format(date) + "'";
        }
    }

    @Override
    protected void createTableLastCheck(IConfig ctable, String tn, StringBuilder sql) {
        sql.append(" ENGINE=InnoDb");
    }

    @Override
    public String escape(String text) {
        String ret = MSql.escape(text);
        if (ret == null || text == null) return ret;
        if (ret.indexOf('\\') < 0) return ret;
        return ret.replaceAll("\\\\", "\\\\\\\\");
    }
}
