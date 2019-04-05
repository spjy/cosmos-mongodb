# MongoDB

An agent to save incoming data from the satellite to a database and communicate with clients to receive that data.

## Installing

```bash
# Clone repository
git clone https://github.com/spjy/cosmos-mongodb.git
```

## Running

1. Open the project in Qt Creator.
2. Run the project by pressing Ctrl + R.

## Options

### Command Line Options

* --whitelist_file_path
  * The file path to the JSON file for a list of whitelisted nodes.
* --include
  * A comma delimited list of nodes as strings to not save data to the database or can contain a wildcard to include all nodes.
* --exclude
  * A comma delimited list of nodes as strings to save data to the database.

**Examples**:

Including and excluding certain nodes:
```bash
--include "cubesat1,hsflpc-03,neutron1" --exclude "node1,node-arduino"
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
