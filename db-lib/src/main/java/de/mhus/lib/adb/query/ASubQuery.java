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

import de.mhus.lib.core.parser.AttributeMap;
import de.mhus.lib.errors.NotSupportedException;

public class ASubQuery extends APart {

    private AAttribute left;
    private AAttribute projection;
    private AQuery<?> subQuery;

    public ASubQuery(AAttribute left, AAttribute projection, AQuery<?> subQuery) {
        this.left = left;
        this.projection = projection;
        this.subQuery = subQuery;
    }

    @Override
    public void getAttributes(AQuery<?> query, AttributeMap map) {
        left.getAttributes(query, map);
        projection.getAttributes(query, map);
        subQuery.getAttributes(query, map);
    }

    public AAttribute getLeft() {
        return left;
    }

    public AAttribute getProjection() {
        return projection;
    }

    public AQuery<?> getSubQuery() {
        return subQuery;
    }

    @Override
    public void append(APart pa) throws NotSupportedException {
        throw new NotSupportedException();
    }
}
