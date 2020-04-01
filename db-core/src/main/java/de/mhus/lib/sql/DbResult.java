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

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import de.mhus.lib.basics.MCloseable;
import de.mhus.lib.core.MDate;

/**
 * Abstract DbResult class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public abstract class DbResult implements MCloseable {

    /**
     * getString.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws java.lang.Exception if any.
     */
    public abstract String getString(String columnLabel) throws Exception;

    /**
     * next.
     *
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public abstract boolean next() throws Exception;

    /**
     * getBinaryStream.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link java.io.InputStream} object.
     * @throws java.lang.Exception if any.
     */
    public abstract InputStream getBinaryStream(String columnLabel) throws Exception;

    /**
     * getBoolean.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public abstract boolean getBoolean(String columnLabel) throws Exception;

    /**
     * getInt.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a int.
     * @throws java.lang.Exception if any.
     */
    public abstract int getInt(String columnLabel) throws Exception;

    /**
     * getLong.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a long.
     * @throws java.lang.Exception if any.
     */
    public abstract long getLong(String columnLabel) throws Exception;

    /**
     * getFloat.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a float.
     * @throws java.lang.Exception if any.
     */
    public abstract float getFloat(String columnLabel) throws Exception;

    /**
     * getDouble.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a double.
     * @throws java.lang.Exception if any.
     */
    public abstract double getDouble(String columnLabel) throws Exception;

    /**
     * getDate.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link java.sql.Date} object.
     * @throws java.lang.Exception if any.
     */
    public abstract Date getDate(String columnLabel) throws Exception;

    /**
     * getMDate.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.core.MDate} object.
     * @throws java.lang.Exception if any.
     */
    public abstract MDate getMDate(String columnLabel) throws Exception;

    /**
     * getTime.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link java.sql.Time} object.
     * @throws java.lang.Exception if any.
     */
    public abstract Time getTime(String columnLabel) throws Exception;

    /**
     * getTimestamp.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link java.sql.Timestamp} object.
     * @throws java.lang.Exception if any.
     */
    public abstract Timestamp getTimestamp(String columnLabel) throws Exception;

    /**
     * getColumnNames.
     *
     * @return a {@link java.util.List} object.
     * @throws java.lang.Exception if any.
     */
    public abstract List<String> getColumnNames() throws Exception;

    /**
     * getObject.
     *
     * @param columnLabel a {@link java.lang.String} object.
     * @return a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public abstract Object getObject(String columnLabel) throws Exception;

    /**
     * get BigDecimal value
     *
     * @param columnLabel
     * @return The value
     * @throws Exception
     */
    public abstract BigDecimal getBigDecimal(String columnLabel) throws Exception;
}
