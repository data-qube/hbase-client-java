# HBase Java Client Example
This is a sample application that shows how to use HBase from the Java API.

The recommended way to configure the connection to HBase is by placing the
corresponding hbase-site.xml under src/main/resources.

Alternatively you can specify the connection properties directly in the code.

## App.java
Shows the basic operations:
- Get
- Put
- Scan

## BufferedMutatorExample.java
Shows how to use batched asynchronous puts for improved performance.
