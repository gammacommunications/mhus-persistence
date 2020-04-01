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

public class ACompare extends APart {

    private TYPE eq;
    private AAttribute left;
    private AAttribute right;

    public ACompare(TYPE eq, AAttribute left, AAttribute right) {
        this.eq = eq;
        this.left = left;
        this.right = right;
    }

    public enum TYPE {
        EQ,
        LT,
        GT,
        EL,
        EG,
        LIKE,
        NE,
        IN,
        LE,
        GE
    }

    @Override
    public void getAttributes(AQuery<?> query, AttributeMap map) {
        left.getAttributes(query, map);
        right.getAttributes(query, map);
    }

    public TYPE getEq() {
        return eq;
    }

    public AAttribute getLeft() {
        return left;
    }

    public AAttribute getRight() {
        return right;
    };

    @Override
    public void append(APart pa) throws NotSupportedException {
        throw new NotSupportedException();
    }
}
