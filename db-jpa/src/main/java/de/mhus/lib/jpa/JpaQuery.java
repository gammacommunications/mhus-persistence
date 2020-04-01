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

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;

/**
 * JpaQuery class.
 *
 * @author mikehummel
 * @version $Id: $Id
 * @param <T>
 */
public class JpaQuery<T> implements TypedQuery<T> {

    private JpaEntityManager entityManager;
    private Query query;

    /**
     * Constructor for JpaQuery.
     *
     * @param entityManager a {@link de.mhus.lib.jpa.JpaEntityManager} object.
     * @param query a {@link javax.persistence.Query} object.
     */
    public JpaQuery(JpaEntityManager entityManager, Query query) {
        this.entityManager = entityManager;
        this.query = query;
    }

    /**
     * Getter for the field <code>entityManager</code>.
     *
     * @return a {@link de.mhus.lib.jpa.JpaEntityManager} object.
     */
    public JpaEntityManager getEntityManager() {
        return entityManager;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public List<T> getResultList() {
        return new JpaList<T>(entityManager, query.getResultList());
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public T getSingleResult() {
        return (T) query.getSingleResult();
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate() {
        return query.executeUpdate();
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setMaxResults(int maxResult) {
        return new JpaQuery<T>(entityManager, query.setMaxResults(maxResult));
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxResults() {
        return query.getMaxResults();
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setFirstResult(int startPosition) {
        return new JpaQuery<T>(entityManager, query.setFirstResult(startPosition));
    }

    /** {@inheritDoc} */
    @Override
    public int getFirstResult() {
        return query.getFirstResult();
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setHint(String hintName, Object value) {
        return new JpaQuery<T>(entityManager, query.setHint(hintName, value));
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, Object> getHints() {
        return query.getHints();
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setParameter(
            Parameter<Calendar> param, Calendar value, TemporalType temporalType) {
        return new JpaQuery<T>(entityManager, query.setParameter(param, value, temporalType));
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
        return new JpaQuery<T>(entityManager, query.setParameter(param, value, temporalType));
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setParameter(String name, Object value) {
        return new JpaQuery<T>(entityManager, query.setParameter(name, value));
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setParameter(String name, Calendar value, TemporalType temporalType) {
        return new JpaQuery<T>(entityManager, query.setParameter(name, value, temporalType));
    }

    /** {@inheritDoc} */
    @Override
    public TypedQuery<T> setParameter(String name, Date value, TemporalType temporalType) {
        return new JpaQuery<T>(entityManager, query.setParameter(name, value, temporalType));
    }

    /** {@inheritDoc} */
    @Override
    public TypedQuery<T> setParameter(int position, Object value) {
        return new JpaQuery<T>(entityManager, query.setParameter(position, value));
    }

    /** {@inheritDoc} */
    @Override
    public TypedQuery<T> setParameter(int position, Calendar value, TemporalType temporalType) {
        return new JpaQuery<T>(entityManager, query.setParameter(position, value, temporalType));
    }

    /** {@inheritDoc} */
    @Override
    public TypedQuery<T> setParameter(int position, Date value, TemporalType temporalType) {
        return new JpaQuery<T>(entityManager, query.setParameter(position, value, temporalType));
    }

    /** {@inheritDoc} */
    @Override
    public Set<Parameter<?>> getParameters() {
        return query.getParameters();
    }

    /** {@inheritDoc} */
    @Override
    public Parameter<?> getParameter(String name) {
        return query.getParameter(name);
    }

    /** {@inheritDoc} */
    @Override
    public <E> Parameter<E> getParameter(String name, Class<E> type) {
        return query.getParameter(name, type);
    }

    /** {@inheritDoc} */
    @Override
    public Parameter<?> getParameter(int position) {
        return query.getParameter(position);
    }

    /** {@inheritDoc} */
    @Override
    public <E> Parameter<E> getParameter(int position, Class<E> type) {
        return query.getParameter(position, type);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isBound(Parameter<?> param) {
        return query.isBound(param);
    }

    /** {@inheritDoc} */
    @Override
    public <E> E getParameterValue(Parameter<E> param) {
        return query.getParameterValue(param);
    }

    /** {@inheritDoc} */
    @Override
    public Object getParameterValue(String name) {
        return query.getParameterValue(name);
    }

    /** {@inheritDoc} */
    @Override
    public Object getParameterValue(int position) {
        return query.getParameterValue(position);
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setFlushMode(FlushModeType flushMode) {
        return new JpaQuery<T>(entityManager, query.setFlushMode(flushMode));
    }

    /** {@inheritDoc} */
    @Override
    public FlushModeType getFlushMode() {
        return query.getFlushMode();
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<T> setLockMode(LockModeType lockMode) {
        return new JpaQuery<T>(entityManager, query.setLockMode(lockMode));
    }

    /** {@inheritDoc} */
    @Override
    public LockModeType getLockMode() {
        return query.getLockMode();
    }

    /** {@inheritDoc} */
    @Override
    public <E> E unwrap(Class<E> cls) {
        return query.unwrap(cls);
    }

    /** {@inheritDoc} */
    @Override
    public <E> TypedQuery<T> setParameter(Parameter<E> param, E value) {
        return new JpaQuery<T>(entityManager, query.setParameter(param, value));
    }
}
