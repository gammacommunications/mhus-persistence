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

import java.net.URI;

import de.mhus.lib.form.MForm;

/**
 * Abstract CaoLoginForm class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public abstract class CaoLoginForm extends MForm {

    /**
     * getURI.
     *
     * @return a {@link java.net.URI} object.
     */
    public abstract URI getURI();

    /**
     * getAuthentication.
     *
     * @return a {@link java.lang.String} object.
     */
    public abstract String getAuthentication();
}
