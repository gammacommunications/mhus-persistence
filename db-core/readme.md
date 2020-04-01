Configurations for SQL Statements:

traceCallers : boolean
	Enable trace of callers stacktrace, creating connections, The trace will be printed to the log if
	a connection was not closed and is closed by finalizer or if a 'too many connections' exceptions
	are thrown.
	
traceCallersWait : long
	How long to wait before printing the ful stacktrace again. This will prevent for filling the log with
	waste.
	
traceRuntime : boolean
	Enable tracing of long running SQL queries. The query and the stacktrace will be written to the log.
	
traceMaxRuntime : long
	Maximum runtime time thats ok, if the query need more time it will be written.
	
