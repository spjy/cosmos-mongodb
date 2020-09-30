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

#ifndef AGENT_MONGO_CPP
#define AGENT_MONGO_CPP

#include <mongo/agent_mongo.h>
#include <mongo/process_files.h>
#include <mongo/process_commands.h>
#include <mongo/maintain_data.h>
#include <mongo/collect_data_loop.h>

// TODO: change include paths so that we can make reference to cosmos using a full path
// example
// #include <cosmos/core/agent/agentclass.h>

static mongocxx::instance instance
{};

static thread collect_data_thread;
static thread soh_walk_thread;
static thread process_files_thread;
static thread process_commands_thread;
static thread maintain_data_thread;
mongocxx::client connection_file;
mongocxx::client connection_command;

int main(int argc, char** argv)
{
    std::vector<std::string> included_nodes;
    std::vector<std::string> excluded_nodes;
    std::string nodes_path;
    std::string file_walk_path = get_cosmosnodes(true);
    std::string agent_path = "~/cosmos/bin/agent";
    std::string shell = "/bin/bash"; // what shell to run execute() function
    std::string mongo_server = "mongodb://localhost:27017/";
    std::string realm = "null"; // encompassing nodes
    std::string collect_mode = "soh"; // soh or agent
    std::string token = "/services/";

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
            else if (strncmp(argv[i], "--collect_mode", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                collect_mode = argv[i + 1];
            }
            else if (strncmp(argv[i], "--slack_token", sizeof(argv[i]) / sizeof(argv[i][0])) == 0) {
                token.insert(token.size(), argv[i + 1]);
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
                cout << "WS Live: Error converting to BSON from JSON" << err.what() << endl;
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
    cout << "Collection mode: " << collect_mode << endl;

    agent = new Agent(node, "mongo");

    if (agent->cinfo == nullptr) {
        cout << "Unable to start agent_mongo" << endl;
        exit(1);
    }
    // Connect to a MongoDB URI and establish connection
    mongocxx::client connection_query {
        mongocxx::uri {
            mongo_server
        }
    };

    connection_file = {
        mongocxx::uri {
            mongo_server
        }
    };

    mongocxx::client connection_live = {
        mongocxx::uri {
            mongo_server
        }
    };

    connection_command = {
        mongocxx::uri {
            mongo_server
        }
    };

    // Create REST API server
    HttpServer query;
    query.config.port = 8082;

    HttpsClient client("hooks.slack.com");

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
    query.resource["^/query/(.+)/(.+)/?$"]["POST"] = [&connection_query, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
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
        mongocxx::collection collection = connection_query[request->path_match[1].str()][request->path_match[2].str()];

        // Parse mongodb options
        if (!options.empty()) {
            set_mongo_options(mongo_options, options);
        }

        // Check if user wants to query multiple documents or a single one
        if (multiple == "t") {
            try {
                // Query the database based on the filter
                query_mtx.lock();
                mongocxx::cursor cursor = collection.find(bsoncxx::from_json(query), mongo_options);
                query_mtx.unlock();

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
                query_mtx.lock();
                document = collection.find_one(bsoncxx::from_json(query), mongo_options);
                query_mtx.unlock();

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
    query.resource["^/command$"]["POST"] = [&agent_path, &shell, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        header.emplace("Content-Type", "text/plain");

        std::string message = request->content.string();
        std::string command = json_extract_namedmember(message, "command");
        std::string type = json_extract_namedmember(message, "type"); // agent or fs

        std::string result;

        if (type == "cosmos") {
            result = execute(agent_path + " " + command, shell);
        } else {
            result = execute(command, shell);
        }

        resp->write(result, header);
    };

    query.resource["^/commands/(.+)?$"]["GET"] = [&connection_query, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();

        // write to cosmos/nodes/node/temp/exec/node_mjd.event
        auto collection = connection_query[realm][request->path_match[1].str() + ":commands"];

        try
        {
            // Get all documents

            query_mtx.lock();
            auto cursor = collection.find({});
            query_mtx.unlock();

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

    query.resource["^/commands/(.+)/?$"]["POST"] = [&connection_query, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();
        std::string command = json_extract_namedmember(message, "command");

        // write to cosmos/nodes/node/temp/exec/node_mjd.event
        auto collection = connection_query[realm][request->path_match[1].str() + ":commands"];

        try
        {
            // Insert BSON object into collection specified
            query_mtx.lock();
            auto insert = collection.insert_one(bsoncxx::from_json(command));
            query_mtx.unlock();

            resp->write("{ \"message\": \"Successfully created command.\" }", header);
        } catch (const mongocxx::bulk_write_exception &err) {
            cout << err.what() << endl;

            resp->write("{\"error\": \" Error inserting into database. \"}", header);
        }
    };


    query.resource["^/commands/(.+)/?$"]["DELETE"] = [&connection_query, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();
        std::string event_name = json_extract_namedmember(message, "event_name");

        event_name.erase(0, 1);
        event_name.pop_back();

        // write to cosmos/nodes/node/temp/exec/node_mjd.event
        auto collection = connection_query[realm][request->path_match[1].str() + ":commands"];

        try
        {
            // Insert BSON object into collection specified
            query_mtx.lock();
            auto del = collection.delete_one(bsoncxx::builder::basic::make_document(kvp("event_name", event_name)));
            query_mtx.unlock();

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
    query.resource["^/exec/(.+)/?$"]["POST"] = [&connection_query, &realm, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
        std::string message = request->content.string();
        std::string event = json_extract_namedmember(message, "event");
        std::string node = request->path_match[1].str();

        // write to cosmos/nodes/node/outgoing/exec/node_mjd.event
        log_write(node, "exec", currentmjd(), "", "command", event.c_str(), "outgoing");

        auto collection = connection_query[realm][request->path_match[1].str() + ":executed"];

        try
        {
            // Insert BSON object into collection specified
            query_mtx.lock();
            auto insert = collection.insert_one(bsoncxx::from_json(event));
            query_mtx.unlock();
        } catch (const mongocxx::bulk_write_exception &err) {
            cout << err.what() << endl;
        }

        resp->write("{ \"message\": \"Successfully generated event file.\" }", header);
    };

    // Get  nodes
    query.resource["^//nodes$"]["GET"] = [&file_walk_path, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
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

    // Get  nodes, processes and pieces
    query.resource["^//agents$"]["GET"] = [&file_walk_path, &header](std::shared_ptr<HttpServer::Response> resp, std::shared_ptr<HttpServer::Request> request) {
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

    // Get  nodes, processes and pieces
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
            cosmosstruc *struc = json_init();
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
                    for (uint i = 0; i < num_pieces; i++) {
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

    // Create a thread for the data collection and service requests.
    collect_data_thread = thread(collect_data_loop, std::ref(connection_live), std::ref(realm), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(collect_mode), std::ref(agent_path), std::ref(shell), std::ref(client), std::ref(token));
    process_files_thread = thread(process_files, std::ref(connection_file), std::ref(realm), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path), "soh");
    process_commands_thread = thread(process_commands, std::ref(connection_command), std::ref(realm), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path), "exec");
    maintain_data_thread = thread(maintain_data, std::ref(agent_path), std::ref(shell), std::ref(included_nodes), std::ref(excluded_nodes), node);

    while(agent->running()) {
        // Sleep for 1 sec
        COSMOS_SLEEP(0.1);
    }

    agent->shutdown();
    collect_data_thread.join();
    process_files_thread.join();
    process_commands_thread.join();
    maintain_data_thread.join();
    query_thread.join();
    ws_live_thread.join();

    return 0;
}

#endif // AGENT_MONGO_CPP
