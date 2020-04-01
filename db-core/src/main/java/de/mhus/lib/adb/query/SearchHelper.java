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
package de.mhus.lib.adb.query;

public class SearchHelper {

    public String findKeyForValue(AQuery<?> query, String value) {
        throw new de.mhus.lib.errors.NotSupportedException(value);
    }

    /**
     * Validate the key. Throw an error to deny the query or return null to ignore this key.
     *
     * @param query
     * @param key
     * @return x
     */
    public String transformKey(AQuery<?> query, String key) {
        return key;
    }

    public String transformValue(AQuery<?> query, String key, String value) {
        return value;
    }

    public boolean isLikeAllowed(AQuery<?> query, String key) {
        return true;
    }

    public APart createEq(String key, String value) {
        return Db.eq(key, value);
    }

    public APart createLike(String key, String value) {
        return Db.like(Db.attr(key), Db.value(null, key, value));
    }
}
