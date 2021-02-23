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
