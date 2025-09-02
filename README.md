# GraphStorage

Comparison of different tools for saving property graphs information in storage.

## Data

Run the `generator.py` script to generate sample data.

## Run the program

### Data ingest
`ingest [NodesFile] [EdgesFile] [PathToDB]`
`mvn compile exec:java -Dexec.mainClass="com.rocks.Main" -Dexec.args="ingest ../Nodes.pgdf ../Edges.pgdf ./databases/n100e250"`

### Querying
`[PathToDB]`
`mvn compile exec:java -Dexec.mainClass="com.rocks.Main" -Dexec.args="./databases/n100e250"`