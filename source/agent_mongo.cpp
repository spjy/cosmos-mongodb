/********************************************************************
* Copyright (C) 2015 by Interstel Technologies, Inc.
*   and Hawaii Space Flight Laboratory.
*
* This file is part of the COSMOS/core that is the central
* module for COSMOS. For more information on COSMOS go to
* <http://cosmos-project.com>
*
* The COSMOS/core software is licenced under the
* GNU Lesser General Public License (LGPL) version 3 licence.
*
* You should have received a copy of the
* GNU Lesser General Public License
* If not, go to <http://www.gnu.org/licenses/>
*
* COSMOS/core is free software: you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public License
* as published by the Free Software Foundation, either version 3 of
* the License, or (at your option) any later version.
*
* COSMOS/core is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* Lesser General Public License for more details.
*
* Refer to the "licences" folder for further information on the
* condititons and terms to use this software.
********************************************************************/

#include <agent_mongo.hpp>

// TODO: change include paths so that we can make reference to cosmos using a full path
// example
// #include <cosmos/core/agent/agentclass.h>

static mongocxx::instance instance
{};

std::string execute(std::string cmd, std::string shell);
void send_live(const std::string type, std::string &node_type, std::string &line);
void collect_data_loop(mongocxx::client &connection_ring, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes);
void file_walk(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path);
void soh_walk(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path);
int32_t request_insert(char* request, char* response, Agent* agent);
int32_t request_cinfo(char* request, char* response, Agent* agent);
static thread collect_data_thread;
static thread file_walk_thread;
static thread soh_walk_thread;
static thread process_files_thread;
static thread process_commands_thread;
static thread maintain_agent_list_thread;
mongocxx::client connection_file;

int main(int argc, char** argv)
{
    std::vector<std::string> included_nodes;
    std::vector<std::string> excluded_nodes;
    std::string nodes_path;
    std::string file_walk_path = get_cosmosnodes(true);
    std::string agent_path = "~/cosmos/bin/agent";
    std::string shell = "/bin/bash";
    std::string mongo_server = "mongodb://localhost:27017/";
    std::string realm = "null";

    char hostname[60];
    gethostname(hostname, sizeof (hostname));
    std::string node = hostname;

    // Get command line arguments for including/excluding certain nodes
    // If include nodes by file, include path to file through --whitelist_file_path
    for (int i = 1; i < argc; i++) {
        // Look for flags and see if the value exists
        if (argv[i][0] == '-' && argv[i][1] == '-' && argv[i + 1] != nullptr) {
            if (strncmp(argv[i], "--include", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                included_nodes = string_split(argv[i + 1], ",");
            }
            else if (strncmp(argv[i], "--exclude", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                excluded_nodes = string_split(argv[i + 1], ",");
            }
            else if (strncmp(argv[i], "--whitelist_file_path", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                nodes_path = argv[i + 1];
            }
            else if (strncmp(argv[i], "--file_walk_path", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                file_walk_path = argv[i + 1];
            }
            else if (strncmp(argv[i], "--agent_path", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                agent_path = argv[i + 1];
            }
            else if (strncmp(argv[i], "--shell", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                shell = argv[i + 1];
            }
            else if (strncmp(argv[i], "--mongo_server", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                mongo_server = argv[i + 1];
            }
            else if (strncmp(argv[i], "--realm", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                realm = argv[i + 1];
            }
            else if (strncmp(argv[i], "--node", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                node = argv[i + 1];
            }
        }
    }

    if (included_nodes.empty() && excluded_nodes.empty() && !nodes_path.empty()) {
        // Open file provided by the command line arg
        ifstream nodes;
        nodes.open(nodes_path, ifstream::binary);

        if (nodes.is_open()) {
            // Get string buffer and extract array values
            std::stringstream buffer;
            buffer << nodes.rdbuf();

            try {
                bsoncxx::document::value json = bsoncxx::from_json(buffer.str());
                bsoncxx::document::view opt { json.view() };

                bsoncxx::document::element include {
                    opt["include"]
                };

                bsoncxx::document::element exclude {
                    opt["exclude"]
                };

                bsoncxx::array::view includesArray {include.get_array().value};
                bsoncxx::array::view excludesArray {exclude.get_array().value};

                for (bsoncxx::array::element e : includesArray) {
                    included_nodes.push_back(bsoncxx::string::to_string(e.get_utf8().value));
                }

                for (bsoncxx::array::element e : excludesArray) {
                    excluded_nodes.push_back(bsoncxx::string::to_string(e.get_utf8().value));
                }
            } catch (const bsoncxx::exception &err) {
                cout << "WS Live: Error converting to BSON from JSON" << endl;
            }
        }
    }

    if (included_nodes.empty() && excluded_nodes.empty() && nodes_path.empty()) {
        included_nodes.push_back("*");
    }

    cout << "Including nodes: ";

    for (std::string s : included_nodes) {
        cout << s + " ";
    }

    cout << endl;

    cout << "Excluding nodes: ";

    for (std::string s : excluded_nodes) {
        cout << s + " ";
    }

    cout << endl << "MongoDB server: " << mongo_server << endl;
    cout << "Inserting into database: " << realm << endl;
    cout << "File walk nodes folder: " << file_walk_path << endl;
    cout << "Agent path: " << agent_path << endl;
    cout << "Shell path: " << shell << endl;
    cout << "Node: " << node << endl;

    agent = new Agent(node, "mongo");

    if (agent->cinfo == nullptr) {
        cout << "Unable to start agent_mongo" << endl;
        exit(1);
    }
    // Connect to a MongoDB URI and establish connection
    mongocxx::client connection_ring {
        mongocxx::uri {
            mongo_server
        }
    };

    connection_file = {
        mongocxx::uri {
            mongo_server
        }
    };

    // Create REST API server
    HttpServer query;
    query.config.port = 8082;

    // Set header fields
    SimpleWeb::CaseInsensitiveMultimap header;
    header.emplace("Content-Type", "application/json");
    header.emplace("Access-Control-Allow-Origin", "*");
    header.emplace("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");
    header.emplace("Access-Control-Max-Age", "1728000");
    header.emplace("Access-Control-Allow-Headers", "*");

    query.default_resource["OPTIONS"] = [&header](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
      try {
          response->write(SimpleWeb::StatusCode::success_ok, "", header);

      }
      catch(const exception &e) {
          response->write(SimpleWeb::StatusCode::client_error_bad_request, e.what());
      }
    };

    // Endpoint is /query/database/node:process, responds with query
    // Payload is { "options": {}, "multiple": bool, "query": {} }
    query.resource["^/query/(.+)/(.+)/?$"]["POST"] = [&connection_ring, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        // Get http payload
        auto message = request->content.string();
        cout << "Query: Received request: " << request->content.string() << endl;
        std::string options = json_extract_namedmember(message, "options");
        std::string multiple = json_extract_namedmember(message, "multiple");
        std::string query = json_extract_namedmember(message, "query");

        bsoncxx::builder::stream::document document {};
        std::string response;
        mongocxx::options::find mongo_options;

        // Connect to db
        mongocxx::collection collection = connection_ring[request->path_match[1].str()][request->path_match[2].str()];

        // Parse mongodb options
        if (!options.empty()) {
            set_mongo_options(mongo_options, options);
        }

        // Check if user wants to query multiple documents or a single one
        if (multiple == "t") {
            try {
                // Query the database based on the filter
                mongocxx::cursor cursor = collection.find(bsoncxx::from_json(query), mongo_options);

                // Check if the returned cursor is empty, if so return an empty array
                if (!(cursor.begin() == cursor.end())) {
                    std::string data;

                    for (auto document : cursor) {
                        data.insert(data.size(), bsoncxx::to_json(document) + ",");
                    }

                    // Rid of dangling comma
                    data.pop_back();

                    // Return response in array
                    response = "[" + data + "]";
                } else if (cursor.begin() == cursor.end() && response.empty()) {
                    response = "[]";
                }
            } catch (const mongocxx::query_exception &err) {
                cout << "WS Query: Logic error when querying occurred" << endl;

                resp->write(SimpleWeb::StatusCode::client_error_bad_request, err.what(), header);
            } catch (const mongocxx::logic_error &err) {
                cout << "WS Query: Logic error when querying occurred" << endl;

                resp->write(SimpleWeb::StatusCode::client_error_bad_request, err.what(), header);
            } catch (const bsoncxx::exception &err) {
                cout << "WS Query: Could not convert JSON" << endl;

                resp->write(SimpleWeb::StatusCode::client_error_bad_request, err.what(), header);
            }
        } else {
            stdx::optional<bsoncxx::document::value> document;

            try {
                // Find single document
                document = collection.find_one(bsoncxx::from_json(query), mongo_options);

                // Check if document is empty, if so return an empty object
                if (document) {
                    std::string data;

                    data = bsoncxx::to_json(document.value());
                    response = data;

                } else if (!document && response.empty()) {
                    response = "{}";
                }
            } catch (const mongocxx::query_exception &err) {
                cout << "WS Query: Logic error when querying occurred" << endl;

                resp->write(SimpleWeb::StatusCode::client_error_bad_request, err.what(), header);
            } catch (const bsoncxx::exception &err) {
                cout << "Could not convert JSON" << endl;

                resp->write(SimpleWeb::StatusCode::client_error_bad_request, err.what(), header);
            }
        }

        if (response.empty()) {
            response = "{ \"error\": \"Empty response.\" }";
        }

        // Return response
        resp->write(response, header);
    };

    // Query agent executable { "command": "agent node proc help" }
    query.resource["^/command$"]["POST"] = [&shell, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        header.emplace("Content-Type", "text/plain");

        std::string message = request->content.string();
        std::string command = json_extract_namedmember(message, "command");

        std::string result = execute(command, shell);

        resp->write(result, header);
    };

    query.resource["^/commands/(.+)?$"]["GET"] = [&connection_ring, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();

        // write to cosmos/nodes/node/temp/exec/node_mjd.event
        auto collection = connection_ring[realm][request->path_match[1].str() + ":commands"];

        try
        {
            // Get all documents
            auto cursor = collection.find({});

            std::ostringstream commands;

            commands << "[";

            for(auto doc : cursor) {
              commands << bsoncxx::to_json(doc) << ",";
            }

            // Remove dangling comma
            if (commands.str().back() == ',') {
                commands.seekp(-1, std::ios_base::end);
            }

            commands << "]";

            resp->write(commands.str(), header);
        } catch (const mongocxx::bulk_write_exception &err) {
            cout << err.what() << endl;

            resp->write("{\"error\": \" Error retrieving from database. \"}", header);
        }

    };

    query.resource["^/commands/(.+)/?$"]["POST"] = [&connection_ring, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();
        std::string command = json_extract_namedmember(message, "command");

        // write to cosmos/nodes/node/temp/exec/node_mjd.event
        auto collection = connection_ring[realm][request->path_match[1].str() + ":commands"];

        try
        {
            // Insert BSON object into collection specified
            auto insert = collection.insert_one(bsoncxx::from_json(command));

            resp->write("{ \"message\": \"Successfully created command.\" }", header);
        } catch (const mongocxx::bulk_write_exception &err) {
            cout << err.what() << endl;

            resp->write("{\"error\": \" Error inserting into database. \"}", header);
        }
    };


    query.resource["^/commands/(.+)/?$"]["DELETE"] = [&connection_ring, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();
        std::string event_name = json_extract_namedmember(message, "event_name");

        event_name.erase(0, 1);
        event_name.pop_back();

        // write to cosmos/nodes/node/temp/exec/node_mjd.event
        auto collection = connection_ring[realm][request->path_match[1].str() + ":commands"];

        try
        {
            // Insert BSON object into collection specified
            auto del = collection.delete_one(bsoncxx::builder::basic::make_document(kvp("event_name", event_name)));

            if (del) {
                resp->write("{ \"message\": \"Successfully deleted command.\" }", header);
            } else {
                resp->write("{\"error\": \" Error deleting. \"}", header);
            }
        } catch (const mongocxx::bulk_write_exception &err) {
            cout << err.what() << endl;

            resp->write("{\"error\": \" Error deleting. \"}", header);
        } catch (...) {
            cout << "Error" << endl;
        }
    };

    // Query agent executable { "command": "agent node proc help" }
    query.resource["^/exec/(.+)/?$"]["POST"] = [&connection_ring, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();
        std::string event = json_extract_namedmember(message, "event");
        std::string node = request->path_match[1].str();

        // write to cosmos/nodes/node/outgoing/exec/node_mjd.event
        log_write(node, "exec", currentmjd(), "", "command", event.c_str(), "outgoing");

        auto collection = connection_ring[realm][request->path_match[1].str() + ":executed"];

        try
        {
            // Insert BSON object into collection specified
            auto insert = collection.insert_one(bsoncxx::from_json(event));
        } catch (const mongocxx::bulk_write_exception &err) {
            cout << err.what() << endl;
        }

        resp->write("{ \"message\": \"Successfully generated event file.\" }", header);
    };

    // Get namespace nodes
    query.resource["^/namespace/nodes$"]["GET"] = [&file_walk_path, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        fs::path nodes = file_walk_path;
        std::ostringstream nodes_list;

        // Array bracket
        nodes_list << "[";

        // Iterate through nodes folder
        for (auto& node: fs::directory_iterator(nodes)) {
            vector<std::string> node_path = string_split(node.path().string(), "/");

            // Get directory by splitting and getting last element in vector
            nodes_list << "\"" + node_path.back() + "\",";
        }

        // Remove dangling comma
        nodes_list.seekp(-1, std::ios_base::end);

        // End array
        nodes_list << "]";

        // Send response
        resp->write(nodes_list.str(), header);
    };

    // Get namespace nodes, processes and pieces
    query.resource["^/namespace/agents$"]["GET"] = [&file_walk_path, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        fs::path nodes = file_walk_path;
        std::ostringstream nodeproc_list;

        // Array bracket
        nodeproc_list << "{";

        // Iterate through nodes folder
        for (auto& node: fs::directory_iterator(nodes)) {
            vector<std::string> node_path = string_split(node.path().string(), "/");

            fs::path incoming = node.path();

            // Read agents on node
            incoming /= "incoming";

            nodeproc_list << "\"" + node_path.back() + "\": [";

            // Loop through the incoming folder
            if (is_directory(incoming)) {
                // Loop through the processes folder
                for (auto& process: fs::directory_iterator(incoming)) {
                    vector<std::string> process_path = string_split(process.path().string(), "/");

                    nodeproc_list << "\"" + process_path.back() + "\",";
                }

                // Remove dangling comma
                nodeproc_list.seekp(-1, std::ios_base::end);
            }

            nodeproc_list << "],";
        }

        // Remove dangling comma
        nodeproc_list.seekp(-1, std::ios_base::end);

        // End array
        nodeproc_list << "}";

        // Send response
        resp->write(nodeproc_list.str(), header);
    };

    // Get namespace nodes, processes and pieces
    query.resource["^/namespace/pieces$"]["GET"] = [&file_walk_path, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        fs::path nodes = file_walk_path;
        std::ostringstream nodeproc_list;

        // Array bracket
        nodeproc_list << "{";

        // Iterate through nodes folder
        for (auto& node: fs::directory_iterator(nodes)) {
            vector<std::string> node_path = string_split(node.path().string(), "/");

            fs::path incoming = node.path();

            // Read pieces file
            fs::path pieces_file = node.path();
            pieces_file /= "pieces.ini";

            nodeproc_list << "\"" + node_path.back() + "\": { \"pieces\": ";

            ifstream pieces;
            std::string node_pieces;

            pieces.open(pieces_file.c_str(), std::ifstream::in);

            while (true) {
                if (!(pieces >> node_pieces)) break;
            }

            nodeproc_list << node_pieces;

            // If empty, return empty object
            if (node_pieces.empty()) {
                nodeproc_list << "{}";
            }

            // Read agents on node
            incoming /= "incoming";

            nodeproc_list << ", \"agents\": [";

            // Loop through the incoming folder
            if (is_directory(incoming)) {
                // Loop through the processes folder
                for (auto& process: fs::directory_iterator(incoming)) {
                    vector<std::string> process_path = string_split(process.path().string(), "/");

                    nodeproc_list << "\"" + process_path.back() + "\",";
                }

                // Remove dangling comma
                nodeproc_list.seekp(-1, std::ios_base::end);
            }

            nodeproc_list << "]},";
        }

        // Remove dangling comma
        nodeproc_list.seekp(-1, std::ios_base::end);

        // End array
        nodeproc_list << "}";

        // Send response
        resp->write(nodeproc_list.str(), header);
    };

    // Get namespace nodes, processes and pieces
    query.resource["^/namespace/all$"]["GET"] = [&file_walk_path, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        fs::path nodes = file_walk_path;
        std::ostringstream nodeproc_list;

        // Array bracket
        nodeproc_list << "{";

        // Iterate through nodes folder
        for (auto& node: fs::directory_iterator(nodes)) {
            std::string node_directory = get_directory(node.path().string());
            fs::path incoming = node.path();

            // Read pieces file
            fs::path pieces_file = node.path();
            pieces_file /= "pieces.ini";

            nodeproc_list << "\"" + node_directory + "\": { \"pieces\":{";

            ifstream pieces;
            std::string node_pieces;
            unsigned int num_pieces = 0;

            pieces.open(pieces_file.c_str(), std::ifstream::in);

            while (true) {
                if (!(pieces >> node_pieces)) break;
            }

            try {
                bsoncxx::document::value json = bsoncxx::from_json(node_pieces);
                bsoncxx::document::view opt { json.view() };

                // coerce into format "piece_name_000": piece_cidx_000
                for (auto e : opt) {
                    std::string key = e.key().to_string();
                    if (key.rfind("piece_name_") == 0) {
                        std::vector<std::string> split = string_split(key, "_");

                        nodeproc_list << "\"" << bsoncxx::string::to_string(e.get_utf8().value) << "\":" << json_extract_namedmember(node_pieces, "piece_cidx_" + split[2]) << ",";

                        num_pieces++;
                    }
                }

                // Remove dangling comma
                if (num_pieces > 0) {
                    nodeproc_list.seekp(-1, std::ios_base::end);
                }
            } catch (const bsoncxx::exception &err) {
                cout << "WS Live: Error converting to BSON from JSON" << endl;
            }

            // Read namespace values
            cosmosstruc *struc = json_create();
            int32_t s = json_setup_node(node_directory, struc);

            nodeproc_list << "}, \"values\":{";

            if (s >= 0 && struc->device.size() == num_pieces) {
                std::string listvalues = json_list_of_all(struc);

                listvalues.erase(0, 1);
                listvalues.pop_back();

                std::string buffer = "{\"values\": [" + listvalues + "]}";

                try {
                    bsoncxx::document::value json = bsoncxx::from_json(buffer);
                    bsoncxx::document::view opt { json.view() };

                    bsoncxx::document::element values {
                        opt["values"]
                    };

                    bsoncxx::array::view valuesArray {values.get_array().value};

                    // didx: { type: [] }
                    std::map<int, std::map<std::string, std::string>> indexed;

                    // organize devices into array according to didx
                    for (bsoncxx::array::element e : valuesArray) {
                        std::string value = bsoncxx::string::to_string(e.get_utf8().value);

                        // Check if it starts with device, exclude device_all
                        if (value.rfind("device_", 0) == 0 && value.rfind("device_all") != 0) {
                            try {
                                int didx = stoi(value.substr(value.find_last_of('_') + 1));
                                std::string type = string_split(value, "_")[1]; // get device type

                                // if empty
                                if (indexed.find(didx) == indexed.end()) {
                                    std::map<std::string, std::string> dtype;
                                    dtype.emplace(type, "[\"" + value + "\",");

                                    indexed.emplace(didx, dtype);
                                } else {
                                    // if not empty, insert into didx:type string
                                    if (indexed[didx].find(type) == indexed[didx].end()) {
                                        indexed[didx].emplace(type, "[\"" + value + "\",");
                                    } else {
                                        indexed[didx][type].insert(indexed[didx][type].size(), "\"" + value + "\",");
                                    }
                                }
                            } catch (const std::invalid_argument &err) {
//                                cout << err.what() << endl;
                            }
                        }
                    }

                    // Remove dangling comma and add ending array bracket to each device array

                    for (std::map<int, std::map<std::string, std::string>>::iterator it = indexed.begin(); it != indexed.end(); ++it) {
                        for (std::map<std::string, std::string>::iterator it_type = it->second.begin(); it_type != it->second.end(); ++it_type) {
                            it_type->second.pop_back();
                            it_type->second.insert(it_type->second.size(), "]");
                        }
                    }

                    // loop through devices, add
                    for (int i = 0; i < num_pieces; i++) {
                        nodeproc_list << "\"" << i << "\":";

                        int didx = struc->device[i].all.didx;
                        std::string type = device_type[struc->device[i].all.type];

                        // If exists
                        if (indexed.find(didx) != indexed.end() && indexed[didx].find(type) != indexed[didx].end()) {
                            nodeproc_list << indexed[didx][type] << ",";
                        } else {
                            nodeproc_list << "[],";
                        }
                    }

                    // Remove dangling comma
                    if (num_pieces > 0) {
                        nodeproc_list.seekp(-1, std::ios_base::end);
                    }
                } catch (const bsoncxx::exception &err) {
                    cout << "WS Live: Error converting to BSON from JSON" << endl;
                }
            }

            // Read agents on node
            incoming /= "incoming";

            nodeproc_list << "}, \"agents\": [";

            // Loop through the incoming folder
            if (is_directory(incoming)) {
                // Loop through the processes folder
                for (auto& process: fs::directory_iterator(incoming)) {
                    vector<std::string> process_path = string_split(process.path().string(), "/");

                    nodeproc_list << "\"" + process_path.back() + "\",";
                }

                // Remove dangling comma
                nodeproc_list.seekp(-1, std::ios_base::end);
            }

            nodeproc_list << "]},";
        }

        // Remove dangling comma
        nodeproc_list.seekp(-1, std::ios_base::end);

        // End array
        nodeproc_list << "}";

        // Send response
        resp->write(nodeproc_list.str(), header);
    };

    // Create websocket server
    WsServer ws_live;
    ws_live.config.port = 8081;

    // Endpoints for querying the database. Goes to /query/
    // Example query message: database=db?collection=test?multiple=true?query={"cost": { "$lt": 11 }}?options={"limit": 5}
    // database: the database name
    // collection: the collection name
    // multiple: whether to return in array format/multiple
    // query: JSON querying the mongo database. See MongoDB docs for more complex queries
    // options: JSON options

    // For live requests, to broadcast to all clients. Goes to /live/node_name/
    auto &echo_all = ws_live.endpoint["^/live/(.+)/?$"];

    echo_all.on_message = [&echo_all](std::shared_ptr<WsServer::Connection> connection, std::shared_ptr<WsServer::InMessage> in_message)
    {
      auto out_message = in_message->string();

      // echo_all.get_connections() can also be used to solely receive connections on this endpoint
      for(auto &endpoint_connections : echo_all.get_connections()) {
          if (connection->path == endpoint_connections->path || endpoint_connections->path == "/live/all" || endpoint_connections->path == "/live/all/") {
              endpoint_connections->send(out_message);
          }
      }
    };

    thread ws_live_thread([&ws_live]() {
      // Start WS-server
      ws_live.start();
    });

    thread query_thread([&query]() {
        // Start server
      query.start();
    });

    int32_t iretn;
    // Add agent request functions
    if ((iretn=agent->add_request("insert", request_insert, "db collection entry_json", "inserts entry_json to collection in db")))
        exit (iretn);

agent->add_request("cinfo", request_cinfo, "cinfo", "get cinfo info");
    // Create a thread for the data collection and service requests.
    collect_data_thread = thread(collect_data_loop, std::ref(connection_ring), std::ref(realm), std::ref(included_nodes), std::ref(excluded_nodes));
//    file_walk_thread = thread(file_walk, std::ref(connection_file), std::ref(database), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path));
    process_files_thread = thread(process_files, std::ref(connection_file), std::ref(realm), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path), "soh");
    process_commands_thread = thread(process_commands, std::ref(connection_file), std::ref(realm), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path), "exec");
    maintain_agent_list_thread = thread(maintain_agent_list, std::ref(included_nodes), std::ref(excluded_nodes), std::ref(agent_path), std::ref(shell));

    while(agent->running()) {
        // Sleep for 1 sec
        COSMOS_SLEEP(0.1);
    }

    agent->shutdown();
    collect_data_thread.join();
//    file_walk_thread.join();
    process_files_thread.join();
    process_commands_thread.join();
    maintain_agent_list_thread.join();
    query_thread.join();
    ws_live_thread.join();

    return 0;
}

//! The method to handle incoming telemetry data to write it to the database
/*!
 *
 * \param connection MongoDB connection instance
 */

void collect_data_loop(mongocxx::client &connection_ring, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes) {
    while (agent->running()) {
        int32_t iretn;

        Agent::messstruc message;
        iretn = agent->readring(message, Agent::AgentMessage::ALL, 1., Agent::Where::TAIL);

        if (iretn > 0) {
            // First use reference to adata to check conditions
            std::string *padata = &message.adata;

            // If no content in adata, don't continue or write to database
            if (!padata->empty() && padata->front() == '{' && padata->back() == '}') {
                // Extract node from jdata
                std::string node = json_extract_namedmember(message.jdata, "agent_node");
                std::string type = json_extract_namedmember(message.jdata, "agent_proc");
                std::string ip = json_extract_namedmember(message.jdata, "agent_addr");

                // Remove leading and trailing quotes around node
                node.erase(0, 1);
                node.pop_back();

                type.erase(0, 1);
                type.pop_back();

                ip.erase(0, 1);
                ip.pop_back();

                std::string node_type = node + ":" + type;

                // Connect to the database and store in the collection of the node name
                if (whitelisted_node(included_nodes, excluded_nodes, node)) {
                    auto collection = connection_ring[realm][node];
                    auto any_collection = connection_ring[realm]["any"];
                    std::string response;
                    mongocxx::options::find options; // store by node

                    bsoncxx::document::view_or_value value;

                    // Copy adata and manipulate string to add the agent_utc (date)
                    std::string adata = message.adata;
                    adata.pop_back();
                    adata.insert(adata.size(), ", \"node_utc\": " + std::to_string(message.meta.beat.utc));
                    adata.insert(adata.size(), ", \"node_type\": \"" + node_type + "\"");
                    adata.insert(adata.size(), ", \"node_ip\": \"" + ip + "\"}");

                    try {
                        // Convert JSON into BSON object to prepare for database insertion
                        value = bsoncxx::from_json(adata);

                        try
                        {
                            // Insert BSON object into collection specified
                            auto insert = collection.insert_one(value);
                            auto any_insert = any_collection.insert_one(value);

                            cout << "WS Live: Inserted adata " << node_type << endl;

                            if (type != "exec") {
                                send_live("WS Live", realm, adata);
                            }
                        } catch (const mongocxx::bulk_write_exception &err) {
                            cout << "WS Live: Error writing to database." << endl;
                        }
                    } catch (const bsoncxx::exception &err) {
                        cout << "WS Live: Error converting to BSON from JSON" << endl;
                    }
                }
            }
        }
        COSMOS_SLEEP(1.);
    }
    return;
}

//! Walk through the directories created by agent_file and process the telemetry data in the incoming folder.
//! \brief file_walk Loop through nodes, then through the incoming folder, then through each process, and finally through each telemetry file.
//! Unzip the file, open it and go line by line and insert the entry into the database. Once done, move the processed file into the archive folder.
//!
void file_walk(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path) {
    // Get the nodes folder
    fs::path nodes = file_walk_path;

    while (agent->running()) {
        cout << "File: Walking through files." << endl;

        // Loop through the nodes folder
        for(auto& node: fs::directory_iterator(nodes)) {
            vector<std::string> node_path = string_split(node.path().string(), "/");
            std::string node_directory = get_directory(node.path().string());

            if (whitelisted_node(included_nodes, excluded_nodes, node_directory)) {
                fs::path incoming = node.path();

                incoming /= "incoming";

                // Loop through the incoming folder
                if (is_directory(incoming)) {
                    // Loop through the processes folder
                    for (auto& process: fs::directory_iterator(incoming)) {
                        vector<std::string> process_path = string_split(process.path().string(), "/");

                        // process soh in another thread, process non-soh processes here
                        if (process_path.back() != "soh") {

                            // Loop through the telemetry files
                            for (auto& telemetry: fs::directory_iterator(process.path())) {

                                // only files with JSON structures
                                if(telemetry.path().filename().string().find(".telemetry") != std::string::npos
                                || telemetry.path().filename().string().find(".event") != std::string::npos
                                || telemetry.path().filename().string().find(".command") != std::string::npos) {
                                    // Uncompress telemetry file
                                    char buffer[8192];
                                    std::string node_type = node_path.back() + ":" + process_path.back();
                                    gzFile gzf = gzopen(telemetry.path().c_str(), "rb");

                                    if (gzf == Z_NULL) {
                                        cout << "File: Error opening " << telemetry.path().c_str() << endl;
                                        // Move the file out of /incoming if we cannot open it
                                        std::string corrupt_file = data_base_path(node_path.back(), "corrupt", process_path.back(), telemetry.path().filename().string());

                                        try {
                                            fs::rename(telemetry, corrupt_file);
                                            cout << "File: Moved corrupt file to" << corrupt_file << endl;
                                        } catch (const std::error_code &error) {
                                            cout << "File: Could not rename file " << error.message() << endl;
                                        }

                                        break;
                                    }

                                    // get the file type

                                    while (!gzeof(gzf))
                                    {
                                        std::string line;

                                        while (!(line.back() == '\n') && !gzeof(gzf))
                                        {
                                            char *nodeString = gzgets(gzf, buffer, 8192);

                                            if (nodeString == Z_NULL) {
                                                cout << "File: Error getting string " << telemetry.path().c_str() << endl;

                                                break;
                                            }

                                            line.append(buffer);
                                        }

                                        // Check if it got to end of file in buffer
                                        if (!gzeof(gzf))
                                        {
                                            // Get the node's UTC
                                            std::string node_utc = json_extract_namedmember(line, "node_utc");

                                            if (node_utc.length() > 0)
                                            {

                                                auto collection = connection_file[realm][node_type];
                                                stdx::optional<bsoncxx::document::value> document;

                                                // Query the database for the node_utc.
                                                try
                                                {
                                                    document = collection.find_one(bsoncxx::from_json("{\"node_utc\":" + node_utc + "}"));
                                                }
                                                catch (const mongocxx::query_exception &err)
                                                {
                                                    cout << "File: Logic error when querying occurred" << endl;
                                                }
                                                catch (const bsoncxx::exception &err)
                                                {
                                                    cout << "File: Could not convert JSON" << endl;
                                                }

                                                // If an entry does not exist with node_utc, write the entry into the database
                                                if (!document)
                                                {
                                                    bsoncxx::document::view_or_value value;

                                                    try
                                                    {
                                                        // Convert JSON into BSON object to prepare for database insertion
                                                        value = bsoncxx::from_json(line);

                                                        try
                                                        {
                                                            // Insert BSON object into collection specified
                                                            auto insert = collection.insert_one(value);

                                                            cout << "File: Inserted adata into collection " << node_type << endl;
                                                        }
                                                        catch (const mongocxx::bulk_write_exception &err)
                                                        {
                                                            cout << "File: Error writing to database." << endl;
                                                        }
                                                    }
                                                    catch (const bsoncxx::exception &err)
                                                    {
                                                        cout << "File: Error converting to BSON from JSON" << endl;
                                                    }
                                                }
                                            }
                                            else //entries that don't have a "node_utc" field
                                            {
                                                node_utc = json_extract_namedmember(line, "event_utc");
                                                if(node_utc.length() > 0 )
                                                {

                                                    auto collection = connection_file[realm][node_type];
                                                    stdx::optional<bsoncxx::document::value> document;
                                                    bsoncxx::document::view_or_value value;

                                                    try
                                                    {
                                                        // Convert JSON into BSON object to prepare for database insertion
                                                        value = bsoncxx::from_json(line);

                                                        try
                                                        {
                                                            // Insert BSON object into collection specified
                                                            auto insert = collection.insert_one(value);

                                                            cout << "File: Inserted adata into collection " << node_type << endl;
                                                        }
                                                        catch (const mongocxx::bulk_write_exception &err)
                                                        {
                                                            cout << "File: Error writing to database." << endl;
                                                        }
                                                    }
                                                    catch (const bsoncxx::exception &err)
                                                    {
                                                        cout << "File: Error converting to BSON from JSON" << endl;
                                                    }

                                                }

                                            }
                                        }
                                    }

                                    gzclose(gzf);
                                }

                                // Move file to archive
                                std::string archive_file = data_base_path(node_path.back(), "archive", process_path.back(), telemetry.path().filename().string());

                                try {
                                    fs::rename(telemetry, archive_file);

                                    cout << "File: Processed file " << telemetry.path() << endl;
                                } catch (const std::error_code &error) {
                                    cout << "File: Could not rename file " << error.message() << endl;
                                }
                            }
                        }
                    }
                }
            }
        }

        cout << "File: Finished walking through files." << endl;

        COSMOS_SLEEP(60);
    }
}

//! Maintain a list of agents and send it through the socket.
//! \brief maintain_agent_list Query the agent list at a certain interval and maintain the list in a sorted set. Send it off to the websocket if anything changes.
//! Execute the agent list_json command, check if node is whitelisted, extract data from json, insert into set, if set is changed then send the update via the live websocket.
//!
void maintain_agent_list(std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &agent_path, std::string &shell) {
    std::set<std::pair<std::string, std::string>> previousSortedAgents;

    std::map<std::string, std::string> fullList;

    while (agent->running())
    {
        std::set<std::pair<std::string, std::string>> sortedAgents;
        std::string list;

        list = execute("\"" + agent_path + " list_json\"", shell);

        WsClient client("localhost:8081/live/list");

        try
        {
            bsoncxx::document::value json = bsoncxx::from_json(list);
            bsoncxx::document::view opt { json.view() };

            bsoncxx::document::element agent_list
            {
                opt["agent_list"]
            };

            bsoncxx::array::view agent_list_array {agent_list.get_array().value};

            for (bsoncxx::array::element e : agent_list_array)
            {
                if (e)
                {
                    bsoncxx::document::element agent_node
                    {
                        e.get_document().view()["agent_node"]
                    };

                    bsoncxx::document::element agent_proc
                    {
                        e.get_document().view()["agent_proc"]
                    };

                    bsoncxx::document::element agent_utc
                    {
                        e.get_document().view()["agent_utc"]
                    };

                    std::string node = bsoncxx::string::to_string(agent_node.get_utf8().value);

                    if (whitelisted_node(included_nodes, excluded_nodes, node))
                    {
                        std::pair<std::string, std::string> agent_node_proc_utc(node + ":" + bsoncxx::string::to_string(agent_proc.get_utf8().value), std::to_string(agent_utc.get_double().value));

                        sortedAgents.insert(agent_node_proc_utc);
                    }
                }
            }

            std::string response = "{\"node_type\": \"list\", \"agent_list\": [";

            std::for_each(fullList.begin(), fullList.end(), [&fullList](const std::pair<std::string, std::string> &item)
            {
                if (fullList[std::get<0>(item)][0] != *"-") {
                   fullList[std::get<0>(item)] = "-" + fullList[std::get<0>(item)];
                }
            });

            std::for_each(sortedAgents.begin(), sortedAgents.end(), [&fullList](const std::pair<std::string, std::string> &item)
            {
                  fullList[std::get<0>(item)] = std::get<1>(item);
            });

            std::for_each(fullList.begin(), fullList.end(), [&response](const std::pair<std::string, std::string> &item)
            {
                response.insert(response.size(), "{\"agent\": \"" + std::get<0>(item) + "\", \"utc\": " + std::get<1>(item) + "},");
            });

            if (response.back() == ',')
            {
                response.pop_back();
            }

            response.insert(response.size(), "]}");

            if (previousSortedAgents != sortedAgents) {
                client.on_open = [&response](std::shared_ptr<WsClient::Connection> connection)
                {
                    cout << "WS Agent Live: Broadcasted updated agent list" << endl;

                    connection->send(response);

                    connection->send_close(1000);
                };

                client.on_close = [](std::shared_ptr<WsClient::Connection> /*connection*/, int status, const std::string & /*reason*/)
                {
                    if (status != 1000) {
                        cout << "WS Live: Closed connection with status code " << status << endl;
                    }
                };

                // See http://www.boost.org/doc/libs/1_55_0/doc/html/boost_asio/reference.html, Error Codes for error code meanings
                client.on_error = [](std::shared_ptr<WsClient::Connection> /*connection*/, const SimpleWeb::error_code &ec)
                {
                    cout << "WS Live: Error: " << ec << ", error message: " << ec.message() << endl;
                };

                client.start();

                previousSortedAgents = sortedAgents;
            }
        }
        catch (const bsoncxx::exception &err)
        {
            cout << "WS Agent Live: Error converting to BSON from JSON" << endl;
        }

        COSMOS_SLEEP(5);
    }
}

int32_t request_insert(char* request, char* response, Agent* agent) {
    std::string req(request);
    req.erase(0, 7);

    size_t first_ws = req.find(' ');
    auto db = req.substr(0, first_ws);
    if (db.length() == 0) {
        sprintf(response, "USAGE: insert database collection entry");
        return 0;
    }
    size_t second_ws = req.substr(first_ws+1).find(' ');
    auto col = req.substr(first_ws+1, second_ws);
    auto entry = req.substr(second_ws +1+db.size());

    auto collection = connection_file[db][col];
    stdx::optional<bsoncxx::document::value> document;
    bsoncxx::document::view_or_value value;

    try
    {
        // Convert JSON into BSON object to prepare for database insertion
        value = bsoncxx::from_json(entry);

        try
        {
            // Insert BSON object into collection specified
            auto insert = collection.insert_one(value);

            cout << "Request: Inserted adata into collection " << col << endl;
            sprintf(response,"Inserted adata into collection %s" ,col.c_str());
        }
        catch (const mongocxx::bulk_write_exception &err)
        {
            cout << "Request: Error writing to database." << endl;
            sprintf(response,"Error writing to database.");
        }
    }
    catch (const bsoncxx::exception &err)
    {
        cout << "Request: Error converting to BSON from JSON ("<<entry<<")" << endl;
        sprintf(response,"Error converting to BSON from JSON.");
    }
    return 0;
}

int32_t request_cinfo(char* request, char* response, Agent* agent) {
    std::string soh_string = "{\"device_bcreg_volt_000\": 7.8933001,\"device_bcreg_amp_000\": 0.0029325001}";
    cosmosstruc *struc = json_create();
    //std::string soh_string = "{\"device_cpu_utc_000\":59015.993214190625,\"device_cpu_maxgib_000\":22.603275,\"device_cpu_gib_000\":11.20945,\"memory_utilization_000\":0.4959214812787105,\"cpu_utilization_000\":1.809999942779541}";

    std::string r(request);
    int32_t s = json_setup_node("neutron1", struc);

    cout << s;

    s = json_parse(soh_string, struc);

    cout << s;

    cout << "omg";

//pieces (50) -> click on another list, gives you name
//        device_cpu_000_...
//        cpu type, know idx 000, assembled names

    return 0;
}
