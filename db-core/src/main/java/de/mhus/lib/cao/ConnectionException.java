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
 * ConnectionException class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class ConnectionException extends CaoException {

    /** */
    private static final long serialVersionUID = 1L;

    /** Constructor for ConnectionException. */
    public ConnectionException() {}

    /**
     * Constructor for ConnectionException.
     *
     * @param message a {@link java.lang.String} object.
     * @param cause a {@link java.lang.Throwable} object.
     */
    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor for ConnectionException.
     *
     * @param message a {@link java.lang.String} object.
     */
    public ConnectionException(String message) {
        super(message);
    }

    /**
     * Constructor for ConnectionException.
     *
     * @param cause a {@link java.lang.Throwable} object.
     */
    public ConnectionException(Throwable cause) {
        super(cause);
    }
}
