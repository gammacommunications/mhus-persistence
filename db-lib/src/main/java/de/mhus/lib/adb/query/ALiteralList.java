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

/**
 * ALiteralList class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class ALiteralList extends APart {

    private APart[] operations;

    /**
     * Constructor for ALiteralList.
     *
     * @param operations a {@link de.mhus.lib.adb.query.APart} object.
     */
    public ALiteralList(APart... operations) {
        this.operations = operations;
    }

    /** {@inheritDoc} */
    @Override
    public void getAttributes(AQuery<?> query, AttributeMap map) {
        for (APart part : operations) part.getAttributes(query, map);
    }

    public APart[] getOperations() {
        return operations;
    }

    @Override
    public void append(APart pa) throws NotSupportedException {
        throw new NotSupportedException();
    }
}
