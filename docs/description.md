
## XDB

There is a basic framework for different db types. its called XDB.

The main Interface is XdbApi, it provide a set of XdbServices and the XdbServices provide XdbTypes.

Actions are done on XdbTypes.

You can provide a XdbApi by creating a 'XdbApi' service.

# ADB

The Adb is a implementation of XDB to handle object persistence. It's similar to JPA but it's not JPA. It's faster and handles direct db connections / pools.

A DBManager is a closed handler to do all the db stuff.

There could be different services with separate DbManagers. You can provide it by creating a 'AdbService' service.

# CommonAdb

Not every bundle wants to instantiate a private DBManager. Therefore a Common DBManager can be provided by the 'CommonAdbService'. It creates a single DBManager and collect all the Entities from Services and let it be managed by the common system.

You can create a 'CommonAdbConsumer' to let your entities be handled bu the CommonAdbService.

