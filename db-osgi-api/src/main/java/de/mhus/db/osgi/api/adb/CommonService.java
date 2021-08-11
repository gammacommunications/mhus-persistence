/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.db.osgi.api.adb;

import java.util.UUID;

import de.mhus.lib.errors.MException;

public interface CommonService {

    void onDelete(Object object);

    <T> T getObject(String type, UUID id) throws MException;

    <T> T getObject(String type, String id) throws MException;

    boolean canRead(Object obj) throws MException;

    boolean canUpdate(Object obj) throws MException;

    boolean canDelete(Object obj) throws MException;

    boolean canCreate(Object obj) throws MException;

    void collectRefereces(Object object, ReferenceCollector collector, String reason);

    <T> T getObject(Class<T> type, UUID id) throws MException;

    String getCommonServiceName();

    CommonDbConsumer getConsumer(String type) throws MException;
}
