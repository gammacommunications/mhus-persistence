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

import de.mhus.lib.core.parser.CompiledString;
import de.mhus.lib.errors.MException;

/**
 * This define a prepared statement connected to a pool. The main differentz to DbStatement, it's
 * not connected to a DbConnection by creation time. But it will precompile the query string.
 *
 * @author mikehummel
 */
public class DbPrepared {

    private DbPool pool;
    private CompiledString query;
    private String original;

    DbPrepared(DbPool pool, String queryString, String language) throws MException {
        this.original = queryString;
        this.pool = pool;
        query = pool.getDialect().getQueryParser(language).compileString(queryString);
        //		query = new SimpleQueryParser().compileString(queryString);
        //		query = new SqlCompiler().compileString(queryString);
    }

    /**
     * Return a statement with a new connection from the pool.
     *
     * @return x
     * @throws Exception
     */
    public DbStatement getStatement() throws Exception {
        DbConnection con = pool.getConnection();
        return con.createStatement(this);
    }

    /**
     * Return a statement for the given connection - hopefully from the same pool.
     *
     * @param con
     * @return x
     * @throws Exception
     */
    public DbStatement getStatement(DbConnection con) throws Exception {
        return con.createStatement(this);
    }

    CompiledString getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return original;
    }
}
