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
package de.mhus.lib.cao.fdb;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoDriver;
import de.mhus.lib.cao.CaoLoginForm;

public class FdbDriver extends CaoDriver {

    @Override
    public CaoCore connect(URI uri, String authentication) {
        try {
            return new FdbCore("fd_" + UUID.randomUUID(), this, new File(uri.getPath()));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CaoLoginForm createLoginForm(URI uri, String authentication) {
        return null;
    }
}
