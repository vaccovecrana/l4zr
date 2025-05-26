Enable JDBC tracing in DBeaver. Add `-Ddbeaver.jdbc.trace=true`.

    nano ./Contents/Eclipse/dbeaver.ini

Tail DBeaver jdbc logs:

    tail -f ./Library/DBeaverData/workspace6/.metadata/jdbc-api-trace.log
