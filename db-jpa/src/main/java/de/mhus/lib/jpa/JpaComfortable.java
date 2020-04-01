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
package de.mhus.lib.jpa;

import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

@MappedSuperclass
public class JpaComfortable implements JpaInjection {

    @Transient protected JpaEntityManager entityManager;

    @Override
    public void doInjectJpa(JpaEntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public boolean isManaged() {
        return entityManager != null && entityManager.isOpen();
    }

    public void save() {
        entityManager.persist(this);
    }

    public void revert() {
        entityManager.refresh(this);
    }

    public Object copy() {
        return entityManager.copy(this);
    }

    public void remove() {
        entityManager.remove(this);
    }

    public void detach() {
        entityManager.detach(this);
    }
}
