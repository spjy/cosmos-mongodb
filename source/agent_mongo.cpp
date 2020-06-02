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

// TODO: change include paths so that we can make reference to cosmos using a full path
// example
// #include <cosmos/core/agent/agentclass.h>

#include "agent/agentclass.h"
#include "support/configCosmos.h"
#include "agent/agentclass.h"
#include "device/serial/serialclass.h"
#include "support/socketlib.h"
#include "support/stringlib.h"
#include "support/jsondef.h"
#include "support/jsonlib.h"
#include "support/datalib.h"

#include <cstdlib>
#include <string.h>
#include <cstring>
#include <iterator>
#include <iostream>
#include <fstream>
#include <thread>
#include <string>
#include <vector>
#include <map>
#include <locale>
#include <memory>
#include <cstdio>
#include <stdexcept>
#include <array>
#include <experimental/filesystem>
#include <set>
#include <tuple>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/types/value.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/options/find.hpp>

#include <server_ws.hpp>
#include <client_ws.hpp>

using WsServer = SimpleWeb::SocketServer<SimpleWeb::WS>;
using WsClient = SimpleWeb::SocketClient<SimpleWeb::WS>;
using namespace bsoncxx;
using bsoncxx::builder::basic::kvp;
using namespace bsoncxx::builder::stream;
namespace fs = std::experimental::filesystem;

static Agent *agent;

static mongocxx::instance instance
{};

//! Options available to specify when querying a Mongo database
enum class MongoFindOption
{
    //! If some shards are unavailable, it returns partial results if true.
    ALLOW_PARTIAL_RESULTS,
    //! The number of documents to return in the first batch.
    BATCH_SIZE,
    //! Specify language specific rules for string comparison.
    COLLATION,
    //! Comment to attach to query to assist in debugging
    COMMENT,
    //! The cursor type
    CURSOR_TYPE,
    //! Specify index name
    HINT,
    //! The limit of how many documents you retrieve.
    LIMIT,
    //! Get the upper bound for an index.
    MAX,
    //! Max time for the server to wait on new documents to satisfy cursor query
    MAX_AWAIT_TIME,
    //! Deprecated
    MAX_SCAN,
    //! Max time for the oepration to run in milliseconds on the server
    MAX_TIME,
    //! Inclusive lower bound for index
    MIN,
    //! Prevent cursor from timing out server side due to activity.
    NO_CURSOR_TIMEOUT,
    //! Projection which limits the returned fields for the matching documents
    PROJECTION,
    //! Read preference
    READ_PREFERENCE,
    //! Deprecated
    MODIFIERS,
    //! Deprecated
    RETURN_KEY,
    //! Whether to include record identifier in results
    SHOW_RECORD_ID,
    //! Specify the number of documents to skip when querying
    SKIP,
    //! Deprecated
    SNAPSHOT,
    //! Order to return the matching documents.
    SORT,
    INVALID
};

std::string execute(std::string cmd, std::string shell);
void collect_data_loop(mongocxx::client &connection_ring, std::string &database, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes);
void file_walk(mongocxx::client &connection_file, std::string &database, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path);
void soh_walk(mongocxx::client &connection_file, std::string &database, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path)
map<std::string, std::string> get_keys(const std::string &request, const std::string variable_delimiter, const std::string value_delimiter);
void str_to_lowercase(std::string &input);
MongoFindOption option_table(std::string input);
void set_mongo_options(mongocxx::options::find &options, std::string request);
void maintain_agent_list(std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &agent_path, std::string &shell);
int32_t request_insert(char* request, char* response, Agent* agent);


static thread collect_data_thread;
static thread file_walk_thread;
static thread soh_walk_thread;
static thread maintain_agent_list_thread;
mongocxx::client connection_file;

/*! Run a command line script and get the output of it.
 * \brief execute Use popen to run a command line script and get the output of the command.
 * \param cmd the command to run
 * \return the output from the command that was run
 */
std::string execute(std::string cmd, std::string shell)
{
    try {
        std::string data;
        FILE * stream;
        const int max_buffer = 256;
        char buffer[max_buffer];
        cmd.insert(0, shell + " -c '");
        cmd.append("'");
        cmd.append(" 2>&1");

        stream = popen(cmd.c_str(), "r");
        if (stream)
        {
            while (!feof(stream))
            {
                if (fgets(buffer, max_buffer, stream) != NULL) data.append(buffer);
            }
            pclose(stream);
        }
        return data;
    } catch (...) {
        return std::string();
    }

}

/*! Check whether a vector contains a certain value.
 * \brief vector_contains loop through the vector and check that a certain value is identical to one inside the vector
 * \param input_vector
 * \param value
 * \return boolean: true if it was found inthe vector, false if not.
 */
bool vector_contains(vector<std::string> &input_vector, std::string value)
{
    for (vector<std::string>::iterator it = input_vector.begin(); it != input_vector.end(); ++it)
    {
        if (*it == value)
        {
            return true;
        }
    }
    return false;
}


/*! Check whether to save data from a node from command line/file specification
 * \brief whitelisted_node loop through the included/excluded vectors and check if the node is contained in either one.
 * \param included_nodes vector of included nodes
 * \param excluded_nodes vector of excluded nodes
 * \param node the node to check against vectors
 * \return whether the node is whitelisted; true if it is, false if it is not.
 */
bool whitelisted_node(vector<std::string> &included_nodes, vector<std::string> &excluded_nodes, std::string &node)
{
    bool whitelisted = false;

    // Check if the node is on the included list, if so return true

    // if not, continue and check if included list contains the wildcard

    // if it contains the wildcard, check if the node is on the excluded list

    if (vector_contains(included_nodes, node))
    {
        whitelisted = true;
    }
    else
    {
        if (vector_contains(excluded_nodes, node))
        {
            whitelisted = false;
        }
        else if (vector_contains(included_nodes, "*"))
        {
            whitelisted = true;
        }
    }

    return whitelisted;
}

//! Retrieve a request consisting of a list of key values and assign them to a map.
 /*! \brief Split up the list of key values by a specified delimiter, then split up the key values by a specified delimiter and assign the key to a map with its corresponding value.
 \param request The request to assign to a map
 \param variable_delimiter The delimiter that separates the list of key values
 \param value_delimiter The delimiter that separates the key from the value
 \return map<std::string, std::string>
*/

map<std::string, std::string> get_keys(const std::string &request, const std::string variable_delimiter, const std::string value_delimiter)
{
    // Delimit by key value pairs
    vector<std::string> input = string_split(request, variable_delimiter);
    map<std::string, std::string> keys;

    // Delimit
    for (vector<std::string>::iterator it = input.begin(); it != input.end(); ++it)
    {
        vector<std::string> kv = string_split(*it, value_delimiter);

        keys[kv[0]] = kv[1]; // Set the variable to the map key and assign the corresponding value
    }

    return keys;
}

//! Convert the characters in a given string to lowercase
/*!
 \param input The string to convert to lowercase
 \return void
*/
void str_to_lowercase(std::string &input)
{
    std::locale locale;
    for (std::string::size_type i = 0; i < input.length(); ++i)
    {
        tolower(input[i], locale);
    }
}

//! Convert a given option and return the enumerated value
/*!
   \param input The option
   \return The enumerated MongoDB find option
*/
MongoFindOption option_table(std::string input)
{
    str_to_lowercase(input);

    if (input == "allow_partial_results") return MongoFindOption::ALLOW_PARTIAL_RESULTS;
    if (input == "batch_size") return MongoFindOption::BATCH_SIZE;
    if (input == "coalition") return MongoFindOption::COLLATION;
    if (input == "comment") return MongoFindOption::COMMENT;
    if (input == "cursor_type") return MongoFindOption::CURSOR_TYPE;
    if (input == "hint") return MongoFindOption::HINT;
    if (input == "limit") return MongoFindOption::LIMIT;
    if (input == "max") return MongoFindOption::MAX;
    if (input == "max_await_time") return MongoFindOption::MAX_AWAIT_TIME;
    if (input == "max_scan") return MongoFindOption::MAX_SCAN;
    if (input == "max_time") return MongoFindOption::MAX_TIME;
    if (input == "min") return MongoFindOption::MIN;
    if (input == "no_cursor_timeout") return MongoFindOption::NO_CURSOR_TIMEOUT;
    if (input == "projection") return MongoFindOption::PROJECTION;
    if (input == "read_preferences") return MongoFindOption::READ_PREFERENCE;
    if (input == "modifiers") return MongoFindOption::MODIFIERS;
    if (input == "return_key") return MongoFindOption::RETURN_KEY;
    if (input == "show_record_id") return MongoFindOption::SHOW_RECORD_ID;
    if (input == "skip") return MongoFindOption::SKIP;
    if (input == "snapshot") return MongoFindOption::SNAPSHOT;
    if (input == "sort") return MongoFindOption::SORT;

    return MongoFindOption::INVALID;
}

//! Set the MongoDB find options in the option class given a JSON object of options
/*!
  \param options The MongoDB find option class to append the options to
  \param request A JSON object of wanted options
*/
void set_mongo_options(mongocxx::options::find &options, std::string request)
{
    try {
	    bsoncxx::document::value json = bsoncxx::from_json(request);
	    bsoncxx::document::view opt { json.view() };

	    for (auto e : opt)
	    {
		std::string key = std::string(e.key());

		MongoFindOption option = option_table(key);

		switch(option)
		{
		    case MongoFindOption::ALLOW_PARTIAL_RESULTS:
			if (e.type() == type::k_int32)
			{
			    options.allow_partial_results(e.get_int32().value);
			}
			else if (e.type() == type::k_int64)
			{
			    options.allow_partial_results(e.get_int64().value);
			}
			break;
		    case MongoFindOption::BATCH_SIZE:
			if (e.type() == type::k_bool) {
			    options.batch_size(e.get_bool().value);
			}
			break;
	//            case MongoFindOption::COLLATION:
	//                options.batch_size(e.get_int32().value); // string view or value
	//                break;
		    case MongoFindOption::LIMIT:
			if (e.type() == type::k_int32)
			{
			    options.limit(e.get_int32().value);
			}
			else if (e.type() == type::k_int64)
			{
			    options.limit(e.get_int64().value);
			}
			break;
		    case MongoFindOption::MAX:
			if (e.type() == type::k_document)
			{
			    options.max(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
			}
			break;
	//            case MongoFindOption::MAX_AWAIT_TIME:
	//                options.max_await_time(e.get_date()); // chronos
	//            case MongoFindOption::MAX_TIME:server
	//                options.max_time() // chronos
		    case MongoFindOption::MIN:
			if (e.type() == type::k_document)
			{
			    options.min(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
			}
			break;
		    case MongoFindOption::NO_CURSOR_TIMEOUT:
			if (e.type() == type::k_bool)
			{
			    options.no_cursor_timeout(e.get_bool().value);
			}
			break;
		    case MongoFindOption::PROJECTION:
			// need to convert document to string then back to document view
			if (e.type() == type::k_document)
			{
			    options.projection(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
			}
			break;
		    case MongoFindOption::RETURN_KEY:
			if (e.type() == type::k_bool)
			{
			    options.return_key(e.get_bool().value);
			}
			break;
		    case MongoFindOption::SHOW_RECORD_ID:
			if (e.type() == type::k_bool)
			{
			    options.show_record_id(e.get_bool().value);
			}
			break;
		    case MongoFindOption::SKIP:
			if (e.type() == type::k_int32)
			{
			    options.skip(e.get_int32().value);
			} else if (e.type() == type::k_int64) {
			    options.skip(e.get_int64().value);
			}
			break;
		    case MongoFindOption::SORT:
			if (e.type() == type::k_document)
			{
			    options.sort(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
			}
			break;
		    default:
			break;
		}
	    }
    } catch (bsoncxx::exception err) {
	cout << err.what() << " - Error parsing MongoDB find options" << endl;
    }

}

int main(int argc, char** argv)
{
    cout << "Agent Mongo" << endl;
    std::string agentname = "mongo";

    std::vector<std::string> included_nodes;
    std::vector<std::string> excluded_nodes;
    std::string nodes_path;
    std::string database = "db";
    std::string file_walk_path = get_cosmosnodes(true);
    std::string agent_path = "~/cosmos/bin/agent";
    std::string shell = "/bin/bash";
    std::string mongo_server = "mongodb://localhost:27017/";

    // Get command line arguments for including/excluding certain nodes
    // If include nodes by file, include path to file through --whitelist_file_path
    for (int i = 1; i < argc; i++)
    {
        // Look for flags and see if the value exists
        if (argv[i][0] == '-' && argv[i][1] == '-' && argv[i + 1] != nullptr)
        {
            if (strncmp(argv[i], "--include", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                included_nodes = string_split(argv[i + 1], ",");
            }
            else if (strncmp(argv[i], "--exclude", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                excluded_nodes = string_split(argv[i + 1], ",");
            }
            else if (strncmp(argv[i], "--whitelist_file_path", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                nodes_path = argv[i + 1];
            }
            else if (strncmp(argv[i], "--database", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                database = argv[i + 1];
            }
            else if (strncmp(argv[i], "--file_walk_path", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                file_walk_path = argv[i + 1];
            }
            else if (strncmp(argv[i], "--agent_path", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                agent_path = argv[i + 1];
            }
            else if (strncmp(argv[i], "--shell", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                shell = argv[i + 1];
            }
            else if (strncmp(argv[i], "--mongo_server", sizeof(argv[i]) / sizeof(argv[i][0])) == 0)
            {
                mongo_server = argv[i + 1];
            }
        }
    }

    if (included_nodes.empty() && excluded_nodes.empty() && !nodes_path.empty())
    {
        // Open file provided by the command line arg
        ifstream nodes;
        nodes.open(nodes_path, ifstream::binary);

        if (nodes.is_open())
        {
            // Get string buffer and extract array values
            std::stringstream buffer;
            buffer << nodes.rdbuf();

            try
            {
                bsoncxx::document::value json = bsoncxx::from_json(buffer.str());
                bsoncxx::document::view opt { json.view() };

                bsoncxx::document::element include
                {
                    opt["include"]
                };

                bsoncxx::document::element exclude
                {
                    opt["exclude"]
                };

                bsoncxx::array::view includesArray {include.get_array().value};
                bsoncxx::array::view excludesArray {exclude.get_array().value};

                for (bsoncxx::array::element e : includesArray)
                {
                    included_nodes.push_back(bsoncxx::string::to_string(e.get_utf8().value));
                }

                for (bsoncxx::array::element e : excludesArray)
                {
                    excluded_nodes.push_back(bsoncxx::string::to_string(e.get_utf8().value));
                }
            }
            catch (bsoncxx::exception err)
            {
                cout << "WS Live: Error converting to BSON from JSON" << endl;
            }
        }
    }

    if (included_nodes.empty() && excluded_nodes.empty() && nodes_path.empty())
    {
        included_nodes.push_back("*");
    }

    cout << "Including nodes: ";

    for (std::string s : included_nodes)
    {
        cout << s + " ";
    }

    cout << endl;

    cout << "Excluding nodes: ";

    for (std::string s : excluded_nodes)
    {
        cout << s + " ";
    }

    cout << endl << "MongoDB server: " << mongo_server << endl;
    cout << "Inserting into database: " << database << endl;
    cout << "File Walk nodes folder: " << file_walk_path << endl;
    cout << "Agent path: " << agent_path << endl;
    cout << "Shell path: " << shell << endl;

    agent = new Agent("", agentname, 1, AGENTMAXBUFFER, false, 20301, NetworkType::UDP, 1);

    if (agent->cinfo == nullptr)
    {
        cout << "Unable to start agent_mongo" << endl;
        exit(1);
    }

    // Connect to a MongoDB URI and establish connection
    mongocxx::client connection_ring
    {
        mongocxx::uri {
            mongo_server
        }
    };

    connection_file = {
        mongocxx::uri {
            mongo_server
        }
    };

    WsServer ws_query;
    WsServer ws_live;
    ws_query.config.port = 8080;
    ws_live.config.port = 8081;

    // Endpoints for querying the database. Goes to /query/
    // Example query message: database=db?collection=test?multiple=true?query={"cost": { "$lt": 11 }}?options={"limit": 5}
    // database: the database name
    // collection: the collection name
    // multiple: whether to return in array format/multiple
    // query: JSON querying the mongo database. See MongoDB docs for more complex queries
    // options: JSON options
    auto &query = ws_query.endpoint["^/query/?$"];

    query.on_message = [&connection_ring](std::shared_ptr<WsServer::Connection> ws_connection, std::shared_ptr<WsServer::InMessage> ws_message)
    {
        std::string message = ws_message->string();

        cout << "WS Query: Received request: " << message << endl;

        bsoncxx::builder::stream::document document {};
        std::string response;
        mongocxx::options::find options;

        map<std::string, std::string> input = get_keys(message, "?", "=");
        mongocxx::collection collection = connection_ring[input["database"]][input["collection"]];

        if (!(input.find("options") == input.end()))
        {
            set_mongo_options(options, input["options"]);
        }

        if (input["multiple"] == "true")
        {
            try
            {
                // Query the database based on the filter
                mongocxx::cursor cursor = collection.find(bsoncxx::from_json(input["query"]), options);

                // Check if the returned cursor is empty, if so return an empty array
                if (!(cursor.begin() == cursor.end()))
                {
                    std::string data;

                    for (auto document : cursor)
                    {
                        data.insert(data.size(), bsoncxx::to_json(document) + ",");
                    }

                    data.pop_back();
                    response = "[" + data + "]";
                }
                else if (cursor.begin() == cursor.end() && response.empty())
                {
                    response = "[]";
                }
            }
            catch (mongocxx::query_exception err)
            {
                cout << "WS Query: Logic error when querying occurred" << endl;

                response = "{\"error\": \"Logic error within the query. Could not query database.\"}";
            }
            catch (mongocxx::logic_error err)
            {
                cout << "WS Query: Logic error when querying occurred" << endl;

                response = "{\"error\": \"Logic error within the query. Could not query database.\"}";
            }
            catch (bsoncxx::exception err)
            {
                cout << "WS Query: Could not convert JSON" << endl;

                response = "{\"error\": \"Improper JSON query.\"}";
            }
        }
        else
        {
            stdx::optional<bsoncxx::document::value> document;
            try
            {
                document = collection.find_one(bsoncxx::from_json(input["query"]), options);

                // Check if document is empty, if so return an empty object
                if (document)
                {
                    std::string data;

                    data = bsoncxx::to_json(document.value());
                    response = data;

                }
                else if (!document && response.empty())
                {
                    response = "{}";
                }
            }
            catch (mongocxx::query_exception err)
            {
                cout << "WS Query: Logic error when querying occurred" << endl;

                response = "{\"error\": \"Logic error within the query. Could not query database.\"}";
            }
            catch (bsoncxx::exception err)
            {
                cout << "Could not convert JSON" << endl;

                response = "{\"error\": \"Improper JSON query.\"}";
            }
        }

        if (response.empty())
        {
            response = "[NOK]";
        }

        ws_connection->send(response, [](const SimpleWeb::error_code &ec)
        {
            if (ec)
            {
                cout << "WS Query: Error sending message. " << ec.message() << endl;
            }
        });
    };

    query.on_open = [](std::shared_ptr<WsServer::Connection> connection)
    {
      cout << "Server: Opened connection " << connection.get() << endl;
      // send token when connected
    };

    query.on_error = [](std::shared_ptr<WsServer::Connection> connection, const SimpleWeb::error_code &ec)
    {
      cout << "WS Query: Error in connection " << connection.get() << ". "
           << "Error: " << ec << ", error message: " << ec.message() << endl;
    };

    // For live requests, to broadcast to all clients. Goes to /live/node_name/
    auto &echo_all = ws_live.endpoint["^/live/(.+)/?$"];

    echo_all.on_message = [&echo_all](std::shared_ptr<WsServer::Connection> connection, std::shared_ptr<WsServer::InMessage> in_message)
    {
      auto out_message = in_message->string();

      // echo_all.get_connections() can also be used to solely receive connections on this endpoint
      for(auto &endpoint_connections : echo_all.get_connections())
      {
          if (connection->path == endpoint_connections->path || endpoint_connections->path == "/live/all" || endpoint_connections->path == "/live/all/")
          {
              endpoint_connections->send(out_message);
          }
      }
    };

    auto &command = ws_query.endpoint["^/command/?$"];

    command.on_message = [&shell](std::shared_ptr<WsServer::Connection> ws_connection, std::shared_ptr<WsServer::InMessage> ws_message)
    {
        std::string message = ws_message->string();

        std::string result = execute(message, shell);

        ws_connection->send(result, [](const SimpleWeb::error_code &ec)
        {
            if (ec) {
                cout << "WS Command: Error sending message. " << ec.message() << endl;
            }
        });
    };

    thread ws_query_thread([&ws_query]()
    {
      // Start WS-server
      ws_query.start();
    });

    thread ws_live_thread([&ws_live]()
    {
      // Start WS-server
      ws_live.start();
    });

    int32_t iretn;
    // Add agent request functions
    if ((iretn=agent->add_request("insert", request_insert, "db collection entry_json", "inserts entry_json to collection in db")))
        exit (iretn);

    // Create a thread for the data collection and service requests.
    collect_data_thread = thread(collect_data_loop, std::ref(connection_ring), std::ref(database), std::ref(included_nodes), std::ref(excluded_nodes));
    file_walk_thread = thread(file_walk, std::ref(connection_file), std::ref(database), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path));
    soh_walk_thread = thread(soh_walk, std::ref(connection_file), std::ref(database), std::ref(included_nodes), std::ref(excluded_nodes), std::ref(file_walk_path));
    maintain_agent_list_thread = thread(maintain_agent_list, std::ref(included_nodes), std::ref(excluded_nodes), std::ref(agent_path), std::ref(shell));

    while(agent->running())
    {
        // Sleep for 1 sec
        COSMOS_SLEEP(0.1);
    }

    agent->shutdown();
    collect_data_thread.join();
    file_walk_thread.join();
    soh_walk_thread.join();
    maintain_agent_list_thread.join();
    ws_query_thread.join();
    ws_live_thread.join();

    return 0;
}

//! The method to handle incoming telemetry data to write it to the database
/*!
 *
 * \param connection MongoDB connection instance
 */
void collect_data_loop(mongocxx::client &connection_ring, std::string &database, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes)
{
    while (agent->running())
    {
        int32_t iretn;

        Agent::messstruc message;
        iretn = agent->readring(message, Agent::AgentMessage::ALL, 1., Agent::Where::TAIL);

        if (iretn > 0)
        {
            // First use reference to adata to check conditions
            std::string *padata = &message.adata;

            // If no content in adata, don't continue or write to database
            if (!padata->empty() && padata->front() == '{' && padata->back() == '}')
            {
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
                if (whitelisted_node(included_nodes, excluded_nodes, node_type))
                {
                    auto collection = connection_ring[database][node_type];
                    std::string response;
                    mongocxx::options::find options; // store by node

                    bsoncxx::document::view_or_value value;

                    // Copy adata and manipulate string to add the agent_utc (date)
                    std::string adata = message.adata;
                    adata.pop_back();
                    adata.insert(adata.size(), ", \"node_utc\": " + std::to_string(message.meta.beat.utc));
                    adata.insert(adata.size(), ", \"node_type\": \"" + node_type + "\"");
                    adata.insert(adata.size(), ", \"node_ip\": \"" + ip + "\"}");

                    try
                    {
                        // Convert JSON into BSON object to prepare for database insertion
                        value = bsoncxx::from_json(adata);

                        try
                        {
                            // Insert BSON object into collection specified
                            auto insert = collection.insert_one(value);

                            cout << "WS Live: Inserted adata into collection " << node_type << endl;

                            if (type != "exec") {
                                std::string ip = "localhost:8081/live/" + node_type;
                                // Websocket client here to broadcast to the WS server, then the WS server broadcasts to all clients that are listening
                                WsClient client(ip);

                                client.on_open = [&adata, &node_type](std::shared_ptr<WsClient::Connection> connection)
                                {
                                    cout << "WS Live: Broadcasted adata for " << node_type << endl;

                                    connection->send(adata);

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
                            }
                        }
                        catch (const mongocxx::bulk_write_exception err)
                        {
                            cout << "WS Live: Error writing to database." << endl;
                        }
                    }
                    catch (const bsoncxx::exception err)
                    {
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
void file_walk(mongocxx::client &connection_file, std::string &database, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path)
{
    // Get the nodes folder
    fs::path nodes = file_walk_path;

    while (agent->running())
    {
        cout << "File: Walking through files." << endl;

        // Loop through the nodes folder
        for(auto& node: fs::directory_iterator(nodes))
        {
            vector<std::string> node_path = string_split(node.path().string(), "/");


            if (whitelisted_node(included_nodes, excluded_nodes, node_path.back()))
            {
                fs::path incoming = node.path();

                incoming /= "incoming";

                // Loop through the incoming folder
                if (is_directory(incoming))
                {
                    // Loop through the processes folder
                    for (auto& process: fs::directory_iterator(incoming)) {

                        // process soh in another thread, process non-soh processes here
                        if (process.path.string() != 'soh') {
                            vector<std::string> process_path = string_split(process.path().string(), "/");

                            // Loop through the telemetry files
                            for (auto& telemetry: fs::directory_iterator(process.path()))
                            {

                                // only files with JSON structures
                                if(telemetry.path().filename().string().find(".telemetry") != std::string::npos
                                || telemetry.path().filename().string().find(".event") != std::string::npos
                                || telemetry.path().filename().string().find(".command") != std::string::npos)
                                {
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
                                            cout << "File: Moved file to" << corrupt_file << endl;
                                        } catch (std::error_code& error) {
                                            cout << "File: Could not rename file " << error.message() << endl;
                                        }

                                        continue;
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

                                                auto collection = connection_file[database][node_type];
                                                stdx::optional<bsoncxx::document::value> document;

                                                // Query the database for the node_utc.
                                                try
                                                {
                                                    document = collection.find_one(bsoncxx::from_json("{\"node_utc\":" + node_utc + "}"));
                                                }
                                                catch (mongocxx::query_exception err)
                                                {
                                                    cout << "File: Logic error when querying occurred" << endl;
                                                }
                                                catch (bsoncxx::exception err)
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
                                                        catch (const mongocxx::bulk_write_exception err)
                                                        {
                                                            cout << "File: Error writing to database." << endl;
                                                        }
                                                    }
                                                    catch (const bsoncxx::exception err)
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

                                                    auto collection = connection_file[database][node_type];
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
                                                        catch (const mongocxx::bulk_write_exception err)
                                                        {
                                                            cout << "File: Error writing to database." << endl;
                                                        }
                                                    }
                                                    catch (const bsoncxx::exception err)
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
                                } catch (std::error_code& error) {
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

//! Walk through the directories created by agent_file and process the telemetry data in the incoming folder.
//! \brief file_walk Loop through nodes, then through the incoming folder, then through each process, and finally through each telemetry file.
//! Unzip the file, open it and go line by line and insert the entry into the database. Once done, move the processed file into the archive folder.
//!
void soh_walk(mongocxx::client &connection_file, std::string &database, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path)
{
    // Get the nodes folder
    fs::path nodes = file_walk_path;

    while (agent->running())
    {
        cout << "SOH Walk: Walking through files." << endl;

        // Loop through the nodes folder
        for(auto& node: fs::directory_iterator(nodes))
        {
            vector<std::string> node_path = string_split(node.path().string(), "/");

            // Check if node is whitelisted
            if (whitelisted_node(included_nodes, excluded_nodes, node_path.back()))
            {
                fs::path soh = node.path();

                // Get SOH folder
                soh /= "incoming" /= "soh";

                // Loop through the soh folder
                if (is_directory(soh))
                {
                    for (auto& telemetry: fs::directory_iterator(soh)) {
                        // only files with JSON structures
                        if(telemetry.path().filename().string().find(".telemetry") != std::string::npos
                        || telemetry.path().filename().string().find(".event") != std::string::npos
                        || telemetry.path().filename().string().find(".command") != std::string::npos)
                        {
                            // Uncompress telemetry file
                            char buffer[8192];
                            
                            std::string node_type = "soh" + ":" + process_path.back();
                            gzFile gzf = gzopen(telemetry.path().c_str(), "rb");

                            if (gzf == Z_NULL) {
                                cout << "File: Error opening " << telemetry.path().c_str() << endl;
                                // Move the file out of /incoming if we cannot open it
                                std::string corrupt_file = data_base_path("soh", "corrupt", process_path.back(), telemetry.path().filename().string());

                                try {
                                    fs::rename(telemetry, corrupt_file);
                                    cout << "File: Moved file to" << corrupt_file << endl;
                                } catch (std::error_code& error) {
                                    cout << "File: Could not rename file " << error.message() << endl;
                                }

                                continue;
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

                                    if(node_utc.length() > 0 )
                                    {

                                        auto collection = connection_file[database][node_type];
                                        stdx::optional<bsoncxx::document::value> document;

                                        // Query the database for the node_utc.
                                        try
                                        {
                                            document = collection.find_one(bsoncxx::from_json("{\"node_utc\":" + node_utc + "}"));
                                        }
                                        catch (mongocxx::query_exception err)
                                        {
                                            cout << "File: Logic error when querying occurred" << endl;
                                        }
                                        catch (bsoncxx::exception err)
                                        {
                                            cout << "File: Could not convert JSON" << endl;
                                        }
                                        
                                        // Append node_type for live values
                                        line.pop_back();
                                        adata.insert(adata.size(), ", \"node_type\": \"" + node_type + "\"}");

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

                                                    std::string ip = "localhost:8081/live/" + node_type;
                                                    // Websocket client here to broadcast to the WS server, then the WS server broadcasts to all clients that are listening
                                                    WsClient client(ip);

                                                    client.on_open = [&line, &node_type](std::shared_ptr<WsClient::Connection> connection)
                                                    {
                                                        cout << "SOH Walk: Broadcasted adata for " << node_type << endl;

                                                        connection->send(line);

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

                                                    cout << "File: Inserted adata into collection " << node_type << endl;
                                                }
                                                catch (const mongocxx::bulk_write_exception err)
                                                {
                                                    cout << "File: Error writing to database." << endl;
                                                }
                                            }
                                            catch (const bsoncxx::exception err)
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

                                            auto collection = connection_file[database][node_type];
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

                                                     std::string ip = "localhost:8081/live/" + node_type;
                                                    // Websocket client here to broadcast to the WS server, then the WS server broadcasts to all clients that are listening
                                                    WsClient client(ip);

                                                    client.on_open = [&line, &node_type](std::shared_ptr<WsClient::Connection> connection)
                                                    {
                                                        cout << "SOH Walk: Broadcasted adata for " << node_type << endl;

                                                        connection->send(line);

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

                                                    cout << "File: Inserted adata into collection " << node_type << endl;
                                                }
                                                catch (const mongocxx::bulk_write_exception err)
                                                {
                                                    cout << "File: Error writing to database." << endl;
                                                }
                                            }
                                            catch (const bsoncxx::exception err)
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
                        } catch (std::error_code& error) {
                            cout << "File: Could not rename file " << error.message() << endl;
                        }
                    }
                }
            }
        }

        cout << "File: Finished walking through files." << endl;

        COSMOS_SLEEP(300);
    }
}

//! Maintain a list of agents and send it through the socket.
//! \brief maintain_agent_list Query the agent list at a certain interval and maintain the list in a sorted set. Send it off to the websocket if anything changes.
//! Execute the agent list_json command, check if node is whitelisted, extract data from json, insert into set, if set is changed then send the update via the live websocket.
//!
void maintain_agent_list(std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &agent_path, std::string &shell) {
    std::set<std::pair<std::string, std::string>> previousSortedAgents;

    while (agent->running())
    {
        std::set<std::pair<std::string, std::string>> sortedAgents;
        std::string list;

        list = execute(agent_path + " list_json", shell);

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

            std::for_each(sortedAgents.begin(), sortedAgents.end(), [&response](const std::pair<std::string, std::string> &item)
            {
                response.insert(response.size(), "{\"agent\": \"" + std::get<0>(item) + "\", \"utc\": \"" + std::get<1>(item) + "\"},");
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
        catch (bsoncxx::exception err)
        {
            cout << "WS Agent Live: Error converting to BSON from JSON" << endl;
        }

        COSMOS_SLEEP(5);
    }

}

int32_t request_insert(char* request, char* response, Agent* agent)
{
    std::string req(request);
    req.erase(0, 7);

    size_t first_ws = req.find(' ');
    auto db = req.substr(0, first_ws);
    if(db.length() == 0){
        sprintf(response,"USAGE: insert database collection entry");
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
        catch (const mongocxx::bulk_write_exception err)
        {
            cout << "Request: Error writing to database." << endl;
            sprintf(response,"Error writing to database.");
        }
    }
    catch (const bsoncxx::exception err)
    {
        cout << "Request: Error converting to BSON from JSON ("<<entry<<")" << endl;
        sprintf(response,"Error converting to BSON from JSON.");
    }
    return 0;
}
