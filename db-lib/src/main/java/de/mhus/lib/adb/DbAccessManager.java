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
package de.mhus.lib.adb;

import de.mhus.lib.adb.model.Table;
import de.mhus.lib.errors.AccessDeniedException;
import de.mhus.lib.sql.DbConnection;

/**
 * Skeleton to allow the validation of access rights. It's created in DbSchema.
 *
 * @author mikehummel
 */
public abstract class DbAccessManager {

    public static final String FEATURE_NAME = "accesscontrol";

    public enum ACCESS {
        READ,
        CREATE,
        UPDATE,
        DELETE
    };

    /**
     * Throws an Exception if the access is not allowed.
     *
     * @param manager
     * @param c
     * @param con
     * @param object
     * @param right
     * @throws AccessDeniedException
     */
    public abstract void hasAccess(
            DbManager manager, Table c, DbConnection con, Object object, ACCESS right)
            throws AccessDeniedException;
}
