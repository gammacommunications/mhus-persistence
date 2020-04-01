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
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.mhus.lib.core.MDate;
import de.mhus.lib.core.logging.MLogUtil;

/**
 * This proxy is used to hold a instance of the connection while the ResultSet is used. That's the
 * only functionality.
 *
 * @author mikehummel
 */
public class JdbcResult extends DbResult {

    private ResultSet instance;
    private DbStatement sth; // need to have a reference to the statement to avoid a finalize
    private List<String> columnNames;

    JdbcResult(DbStatement sth, ResultSet instance) {
        this.sth = sth;
        this.instance = instance;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return instance.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return instance.isWrapperFor(iface);
    }

    @Override
    public boolean next() throws SQLException {
        return instance.next();
    }

    @Override
    public void close() {
        try {
            instance.close();
        } catch (SQLException e) {
            MLogUtil.log().d(this, e);
        }
    }

    public boolean wasNull() throws SQLException {
        return instance.wasNull();
    }

    public String getString(int columnIndex) throws SQLException {
        return instance.getString(columnIndex);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return instance.getBoolean(columnIndex);
    }

    public byte getByte(int columnIndex) throws SQLException {
        return instance.getByte(columnIndex);
    }

    public short getShort(int columnIndex) throws SQLException {
        return instance.getShort(columnIndex);
    }

    public int getInt(int columnIndex) throws SQLException {
        return instance.getInt(columnIndex);
    }

    public long getLong(int columnIndex) throws SQLException {
        return instance.getLong(columnIndex);
    }

    public float getFloat(int columnIndex) throws SQLException {
        return instance.getFloat(columnIndex);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return instance.getDouble(columnIndex);
    }

    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return instance.getBigDecimal(columnIndex, scale);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return instance.getBytes(columnIndex);
    }

    public Date getDate(int columnIndex) throws SQLException {
        return instance.getDate(columnIndex);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return instance.getTime(columnIndex);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return instance.getTimestamp(columnIndex);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return instance.getAsciiStream(columnIndex);
    }

    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return instance.getUnicodeStream(columnIndex);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return instance.getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return instance.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return instance.getBoolean(columnLabel);
    }

    public byte getByte(String columnLabel) throws SQLException {
        return instance.getByte(columnLabel);
    }

    public short getShort(String columnLabel) throws SQLException {
        return instance.getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return instance.getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return instance.getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return instance.getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return instance.getDouble(columnLabel);
    }

    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return instance.getBigDecimal(columnLabel, scale);
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return instance.getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return instance.getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return instance.getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return instance.getTimestamp(columnLabel);
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return instance.getAsciiStream(columnLabel);
    }

    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return instance.getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return instance.getBinaryStream(columnLabel);
    }

    public SQLWarning getWarnings() throws SQLException {
        return instance.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        instance.clearWarnings();
    }

    public String getCursorName() throws SQLException {
        return instance.getCursorName();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return instance.getMetaData();
    }

    public Object getObject(int columnIndex) throws SQLException {
        return instance.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return instance.getObject(columnLabel);
    }

    public int findColumn(String columnLabel) throws SQLException {
        return instance.findColumn(columnLabel);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return instance.getCharacterStream(columnIndex);
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return instance.getCharacterStream(columnLabel);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return instance.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return instance.getBigDecimal(columnLabel);
    }

    public boolean isBeforeFirst() throws SQLException {
        return instance.isBeforeFirst();
    }

    public boolean isAfterLast() throws SQLException {
        return instance.isAfterLast();
    }

    public boolean isFirst() throws SQLException {
        return instance.isFirst();
    }

    public boolean isLast() throws SQLException {
        return instance.isLast();
    }

    public void beforeFirst() throws SQLException {
        instance.beforeFirst();
    }

    public void afterLast() throws SQLException {
        instance.afterLast();
    }

    public boolean first() throws SQLException {
        return instance.first();
    }

    public boolean last() throws SQLException {
        return instance.last();
    }

    public int getRow() throws SQLException {
        return instance.getRow();
    }

    public boolean absolute(int row) throws SQLException {
        return instance.absolute(row);
    }

    public boolean relative(int rows) throws SQLException {
        return instance.relative(rows);
    }

    public boolean previous() throws SQLException {
        return instance.previous();
    }

    public void setFetchDirection(int direction) throws SQLException {
        instance.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return instance.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        instance.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return instance.getFetchSize();
    }

    public int getType() throws SQLException {
        return instance.getType();
    }

    public int getConcurrency() throws SQLException {
        return instance.getConcurrency();
    }

    public boolean rowUpdated() throws SQLException {
        return instance.rowUpdated();
    }

    public boolean rowInserted() throws SQLException {
        return instance.rowInserted();
    }

    public boolean rowDeleted() throws SQLException {
        return instance.rowDeleted();
    }

    public void updateNull(int columnIndex) throws SQLException {
        instance.updateNull(columnIndex);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        instance.updateBoolean(columnIndex, x);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        instance.updateByte(columnIndex, x);
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        instance.updateShort(columnIndex, x);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        instance.updateInt(columnIndex, x);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        instance.updateLong(columnIndex, x);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        instance.updateFloat(columnIndex, x);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        instance.updateDouble(columnIndex, x);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        instance.updateBigDecimal(columnIndex, x);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        instance.updateString(columnIndex, x);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        instance.updateBytes(columnIndex, x);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        instance.updateDate(columnIndex, x);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        instance.updateTime(columnIndex, x);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        instance.updateTimestamp(columnIndex, x);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        instance.updateAsciiStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        instance.updateBinaryStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        instance.updateCharacterStream(columnIndex, x, length);
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        instance.updateObject(columnIndex, x, scaleOrLength);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        instance.updateObject(columnIndex, x);
    }

    public void updateNull(String columnLabel) throws SQLException {
        instance.updateNull(columnLabel);
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        instance.updateBoolean(columnLabel, x);
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        instance.updateByte(columnLabel, x);
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        instance.updateShort(columnLabel, x);
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        instance.updateInt(columnLabel, x);
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        instance.updateLong(columnLabel, x);
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        instance.updateFloat(columnLabel, x);
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        instance.updateDouble(columnLabel, x);
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        instance.updateBigDecimal(columnLabel, x);
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        instance.updateString(columnLabel, x);
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        instance.updateBytes(columnLabel, x);
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        instance.updateDate(columnLabel, x);
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        instance.updateTime(columnLabel, x);
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        instance.updateTimestamp(columnLabel, x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        instance.updateAsciiStream(columnLabel, x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        instance.updateBinaryStream(columnLabel, x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException {
        instance.updateCharacterStream(columnLabel, reader, length);
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        instance.updateObject(columnLabel, x, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        instance.updateObject(columnLabel, x);
    }

    public void insertRow() throws SQLException {
        instance.insertRow();
    }

    public void updateRow() throws SQLException {
        instance.updateRow();
    }

    public void deleteRow() throws SQLException {
        instance.deleteRow();
    }

    public void refreshRow() throws SQLException {
        instance.refreshRow();
    }

    public void cancelRowUpdates() throws SQLException {
        instance.cancelRowUpdates();
    }

    public void moveToInsertRow() throws SQLException {
        instance.moveToInsertRow();
    }

    public void moveToCurrentRow() throws SQLException {
        instance.moveToCurrentRow();
    }

    public Statement getStatement() throws SQLException {
        return instance.getStatement();
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return instance.getObject(columnIndex, map);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        return instance.getRef(columnIndex);
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        return instance.getBlob(columnIndex);
    }

    public Clob getClob(int columnIndex) throws SQLException {
        return instance.getClob(columnIndex);
    }

    public Array getArray(int columnIndex) throws SQLException {
        return instance.getArray(columnIndex);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return instance.getObject(columnLabel, map);
    }

    public Ref getRef(String columnLabel) throws SQLException {
        return instance.getRef(columnLabel);
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        return instance.getBlob(columnLabel);
    }

    public Clob getClob(String columnLabel) throws SQLException {
        return instance.getClob(columnLabel);
    }

    public Array getArray(String columnLabel) throws SQLException {
        return instance.getArray(columnLabel);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return instance.getDate(columnIndex, cal);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return instance.getDate(columnLabel, cal);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return instance.getTime(columnIndex, cal);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return instance.getTime(columnLabel, cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return instance.getTimestamp(columnIndex, cal);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return instance.getTimestamp(columnLabel, cal);
    }

    public URL getURL(int columnIndex) throws SQLException {
        return instance.getURL(columnIndex);
    }

    public URL getURL(String columnLabel) throws SQLException {
        return instance.getURL(columnLabel);
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        instance.updateRef(columnIndex, x);
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        instance.updateRef(columnLabel, x);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        instance.updateBlob(columnIndex, x);
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        instance.updateBlob(columnLabel, x);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        instance.updateClob(columnIndex, x);
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        instance.updateClob(columnLabel, x);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        instance.updateArray(columnIndex, x);
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        instance.updateArray(columnLabel, x);
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        return instance.getRowId(columnIndex);
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        return instance.getRowId(columnLabel);
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        instance.updateRowId(columnIndex, x);
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        instance.updateRowId(columnLabel, x);
    }

    public int getHoldability() throws SQLException {
        return instance.getHoldability();
    }

    public boolean isClosed() throws SQLException {
        return instance.isClosed();
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        instance.updateNString(columnIndex, nString);
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        instance.updateNString(columnLabel, nString);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        instance.updateNClob(columnIndex, nClob);
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        instance.updateNClob(columnLabel, nClob);
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        return instance.getNClob(columnIndex);
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        return instance.getNClob(columnLabel);
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return instance.getSQLXML(columnIndex);
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return instance.getSQLXML(columnLabel);
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        instance.updateSQLXML(columnIndex, xmlObject);
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        instance.updateSQLXML(columnLabel, xmlObject);
    }

    public String getNString(int columnIndex) throws SQLException {
        return instance.getNString(columnIndex);
    }

    public String getNString(String columnLabel) throws SQLException {
        return instance.getNString(columnLabel);
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return instance.getNCharacterStream(columnIndex);
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return instance.getNCharacterStream(columnLabel);
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        instance.updateNCharacterStream(columnIndex, x, length);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        instance.updateNCharacterStream(columnLabel, reader, length);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        instance.updateAsciiStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        instance.updateBinaryStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        instance.updateCharacterStream(columnIndex, x, length);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        instance.updateAsciiStream(columnLabel, x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        instance.updateBinaryStream(columnLabel, x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        instance.updateCharacterStream(columnLabel, reader, length);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        instance.updateBlob(columnIndex, inputStream, length);
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException {
        instance.updateBlob(columnLabel, inputStream, length);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        instance.updateClob(columnIndex, reader, length);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        instance.updateClob(columnLabel, reader, length);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        instance.updateNClob(columnIndex, reader, length);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        instance.updateNClob(columnLabel, reader, length);
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        instance.updateNCharacterStream(columnIndex, x);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        instance.updateNCharacterStream(columnLabel, reader);
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        instance.updateAsciiStream(columnIndex, x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        instance.updateBinaryStream(columnIndex, x);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        instance.updateCharacterStream(columnIndex, x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        instance.updateAsciiStream(columnLabel, x);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        instance.updateBinaryStream(columnLabel, x);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        instance.updateCharacterStream(columnLabel, reader);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        instance.updateBlob(columnIndex, inputStream);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        instance.updateBlob(columnLabel, inputStream);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        instance.updateClob(columnIndex, reader);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        instance.updateClob(columnLabel, reader);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        instance.updateNClob(columnIndex, reader);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        instance.updateNClob(columnLabel, reader);
    }

    public DbConnection getConnection() {
        return sth.getConnection();
    }

    @Override
    public MDate getMDate(String columnLabel) throws Exception {
        return new MDate(getTimestamp(columnLabel));
    }

    @Override
    public List<String> getColumnNames() throws Exception {
        if (columnNames == null) {
            LinkedList<String> l = new LinkedList<String>();
            ResultSetMetaData meta = instance.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String name = meta.getColumnName(i);
                l.add(name);
            }
            columnNames = Collections.unmodifiableList(l);
        }
        return columnNames;
    }
}
