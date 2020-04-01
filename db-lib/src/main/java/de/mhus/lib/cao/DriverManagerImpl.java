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
 * DriverManagerImpl class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DriverManagerImpl extends CaoDriverManager {

    /**
     * register.
     *
     * @param driver a {@link de.mhus.lib.cao.CaoDriver} object.
     */
    public void register(CaoDriver driver) {
        schemes.put(driver.getScheme(), driver);
    }

    /**
     * unregister.
     *
     * @param driver a {@link de.mhus.lib.cao.CaoDriver} object.
     */
    public void unregister(CaoDriver driver) {
        schemes.remove(driver.getScheme());
    }

    /**
     * unregister.
     *
     * @param scheme a {@link java.lang.String} object.
     */
    @SuppressWarnings("unlikely-arg-type")
    public void unregister(String scheme) {
        schemes.remove(schemes);
    }
}
