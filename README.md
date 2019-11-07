# MongoDB

An agent to save incoming data from the satellite to a database and communicate with clients to receive that data.

## Requirements

* C++17
* [GCC/G++ 7.4.0+](https://gist.github.com/jlblancoc/99521194aba975286c80f93e47966dc5)
* [Crypto](https://github.com/openssl/openssl)
* [Boost](https://www.boost.org/doc/libs/1_66_0/more/getting_started/unix-variants.html)
* [MongoDB Community Server](https://www.mongodb.com/download-center/community)

## Installing

```bash
git clone https://github.com/spjy/cosmos-mongodb.git # clone into cosmos/source/projects/
cd cosmos-mongodb

# Make build folder
mkdir build
cd build

# Compile
cmake ../source
make
```

## Running

```
./agent_mongo --database database_name
```

## Options

### Websocket

The websocket query listening port is 8080. These are socket requests are used to get data from the agent.
The websocket live listening port is 8081. These socket requests are used to get data as it is coming in.

#### /query/
You may send a query string to this endpoint to query the MongoDB database.

**Body**: database=db?collection=coll?multiple=true?query={"key"}?options={"opt": "val"}

**Options**:
* database (the database name to query)
* collection (the collection name to query)
* multiple (whether to return an object or array
* query (the JSON to query the MongoDB database; see MongoDB docs for more complex queries)
* options (options provided by MongoDB)

**Return**: Rows from the database specified in the body in JSON format.

#### /command/
Initialize an agent request, e.g. `agent list`. It runs the command in the command line and returns the output. Omit the `agent ` prefix and only include the command after that.

**Body**: command

**Options**: -

**Return**: The output of the agent request.

#### /live/
This is a listen-only endpoint. As data is flowing in from any node/process, it will be sent out on this endpoint.

**Body**: None

**Options**: None

**Return**: Data from every node/process in JSON format.



### Command Line Options

* --whitelist_file_path
  * The file path to the JSON file for a list of whitelisted nodes.
* --include
  * A comma delimited list of nodes as strings to not save data to the database or can contain a wildcard to include all nodes.
* --exclude
  * A comma delimited list of nodes as strings to save data to the database.
* --database
  * The database to save agent data to.

**Examples**:

Including and excluding certain nodes:
```bash
--include "cubesat1,hsflpc-03,neutron1" --exclude "node1,node-arduino" --database "agent_dump"
```

Including all and excluding certain nodes:
```bash
--include "*" --exclude "node1,node-arduino"
```

Specifying nodes from a file:
```bash
--whitelist_file_path "/home/usr/cosmos/source/projects/mongodb/source/nodes.json"
```

### Whitelist File Format

The whitelist file is a JSON file and will be imported using the command line option as demonstrated above.

Including and excluding certain nodes:
```json
{
  "include": ["cubesat1", "hsflpc-03", "neutron1"],
  "exclude": ["node1", "node-arduino"]
}
```

Including all and excluding certain nodes:
```json
{
  "include": ["*"],
  "exclude": ["node1", "node-arduino"]
}
```
