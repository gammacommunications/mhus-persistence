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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import de.mhus.lib.core.parser.CompiledString;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.analytics.SqlAnalytics;

/**
 * This represents a qyery statement. Use it to execute queries.
 *
 * @author mikehummel
 */
public class JdbcStatement extends DbStatement {

    private JdbcConnection dbCon;
    private Statement sth;

    private CompiledString query;

    private PreparedStatement preparedSth;

    private String xquery;
    private String original;

    JdbcStatement(JdbcConnection dbCon, DbPrepared prepared) {
        this.original = prepared.toString();
        this.dbCon = dbCon;
        this.query = prepared.getQuery();
    }

    JdbcStatement(JdbcConnection dbCon, String query, String language) throws MException {
        this.original = query;
        this.dbCon = dbCon;
        this.query = dbCon.createQueryCompiler(language).compileString(query);
    }

    private void validateSth() throws Exception {
        synchronized (this) {
            if (sth == null || sth.isClosed()) {
                Connection con = dbCon.getConnection();
                sth = con.createStatement();
            }
        }
    }

    protected PreparedStatement prepareStatement(
            Map<String, Object> attributes, Statement sth, String query) throws SQLException {

        // recycle prepared query - should not differ !
        if (xquery != null & preparedSth != null && xquery.equals(query)) return preparedSth;

        // if differ close last prepared query
        closePreparedSth();

        // checkout new
        if (attributes != null && attributes.containsKey(RETURN_BINARY_KEY + "0")) {
            PreparedStatement psth = dbCon.getConnection().prepareStatement(query);
            xquery = query;
            for (int nr = 0; attributes.containsKey(RETURN_BINARY_KEY + nr); nr++) {
                psth.setBinaryStream(nr + 1, (InputStream) attributes.get(RETURN_BINARY_KEY + nr));
                attributes.remove(RETURN_BINARY_KEY + nr);
            }
            return psth;
        }
        return null;
    }

    protected void closePreparedSth() {
        if (preparedSth != null) {
            xquery = null;
            try {
                preparedSth.close();
            } catch (SQLException e) {
                log().t(e);
            }
            preparedSth = null;
        }
    }

    /**
     * Executes the given SQL statement, which may return multiple results. In this statement
     * InputStream as attribute values are allowed. @See Statement.execute
     *
     * @param attributes
     * @return x
     * @throws Exception
     */
    @Override
    public boolean execute(Map<String, Object> attributes) throws Exception {
        validateSth();
        String query = this.query.execute(attributes);
        log().t(query);
        long start = System.currentTimeMillis();
        try {
            preparedSth = prepareStatement(attributes, sth, query);
            boolean result = preparedSth == null ? sth.execute(query) : preparedSth.execute();
            SqlAnalytics.trace(getConnection().getInstanceId(), original, query, start, null);
            return result;
        } catch (Throwable e) {
            SqlAnalytics.trace(getConnection().getInstanceId(), original, query, start, e);
            log().e(query);
            throw e;
        }
    }

    @Override
    public DbResult getResultSet() throws SQLException {
        return new JdbcResult(this, sth.getResultSet());
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return sth.getUpdateCount();
    }

    /**
     * Return the result of an select query.
     *
     * @param attributes
     * @return x
     * @throws Exception
     */
    @Override
    public DbResult executeQuery(Map<String, Object> attributes) throws Exception {
        validateSth();
        String query = this.query.execute(attributes);
        log().t(query);
        preparedSth = prepareStatement(attributes, sth, query);
        long start = System.currentTimeMillis();
        try {
            ResultSet result =
                    preparedSth == null ? sth.executeQuery(query) : preparedSth.executeQuery();
            SqlAnalytics.trace(getConnection().getInstanceId(), original, query, start, null);
            return new JdbcResult(this, result);
        } catch (Throwable t) {
            SqlAnalytics.trace(getConnection().getInstanceId(), original, query, start, t);
            log().e(query);
            throw t;
        }
    }

    /**
     * Return the result of an update query. In the attributes InputStreams are allowed (blobs).
     *
     * @param attributes
     * @return x
     * @throws Exception
     */
    @Override
    public int executeUpdate(Map<String, Object> attributes) throws Exception {
        validateSth();
        String query = this.query.execute(attributes);
        log().t(query);
        preparedSth = prepareStatement(attributes, sth, query);
        long start = System.currentTimeMillis();
        try {
            int result =
                    preparedSth == null ? sth.executeUpdate(query) : preparedSth.executeUpdate();
            SqlAnalytics.trace(getConnection().getInstanceId(), original, query, start, null);
            return result;
        } catch (Throwable t) {
            SqlAnalytics.trace(getConnection().getInstanceId(), original, query, start, t);
            log().e(query);
            throw t;
        }
    }

    /**
     * Return the used connection.
     *
     * @return x
     */
    @Override
    public DbConnection getConnection() {
        return dbCon;
    }

    @Override
    public void close() {
        closePreparedSth();
        if (sth == null) return;
        try {
            if (sth != null && !sth.isClosed()) {
                sth.close();
            }
        } catch (Exception e) {
            log().i(e);
        }
        sth = null;
    }

    @Override
    public String toString() {
        return original;
    }
}
