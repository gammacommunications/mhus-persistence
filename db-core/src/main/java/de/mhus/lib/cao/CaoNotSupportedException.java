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
package de.mhus.lib.cao;

/**
 * CaoNotSupportedException class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class CaoNotSupportedException extends CaoException {

    /** */
    private static final long serialVersionUID = 1L;

    /**
     * Constructor for CaoNotSupportedException.
     *
     * @param in a {@link java.lang.Object} object.
     */
    public CaoNotSupportedException(Object... in) {
        super(in);
    }
}
