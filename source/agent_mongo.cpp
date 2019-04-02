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
#include <string.h>
#include <iterator>
#include <iostream>
#include <fstream>
#include <thread>
#include <string>
#include <vector>
#include <map>
#include <locale>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/array/element.hpp>
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

using namespace bsoncxx;
using bsoncxx::builder::basic::kvp;
using namespace bsoncxx::builder::stream;

static Agent *agent;

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

void collect_data_loop(mongocxx::client &connection);
void service_requests(mongocxx::client &connection);
map<std::string, std::string> get_keys(std::string &request, const std::string variable_delimiter, const std::string value_delimiter);
void str_to_lowercase(std::string &input);
MongoFindOption option_table(std::string input);
void set_mongo_options(mongocxx::options::find &options, std::string request);

static std::thread collect_data_thread;
static std::thread service_requests_thread;

//! Retrieve a request consisting of a list of key values and assign them to a map.
 /*! Split up the list of key values by a specified delimiter, then split up the key values by a specified delimiter and assign the key to a map with its corresponding value.
 \param request The request to assign to a map
 \param variable_delimiter The delimiter that separates the list of key values
 \param value_delimiter The delimiter that separates the key from the value
 \return map<std::string, std::string>
*/

map<std::string, std::string> get_keys(std::string &request, const std::string variable_delimiter, const std::string value_delimiter)
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
        std::tolower(input[i], locale);
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
void set_mongo_options(mongocxx::options::find &options, std::string request) {
    bsoncxx::document::value reqJSON = bsoncxx::from_json(request);
    bsoncxx::document::view opt { reqJSON.view() };

    for (auto e : opt)
    {
        std::string key = std::string(e.key());

        MongoFindOption option = option_table(key);

        switch(option) {
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
//            case MongoFindOption::MAX:
//                options.max(e.get_document()); // bson view or value
//            case MongoFindOption::MAX_AWAIT_TIME:
//                options.max_await_time(e.get_date()); // chronos
//            case MongoFindOption::MAX_TIME:
//                options.max_time() // chronos
//            case MongoFindOption::MIN:
//                options.min(e.get_document()) // bson view or value
            case MongoFindOption::NO_CURSOR_TIMEOUT:
                if (e.type() == type::k_bool) {
                   options.no_cursor_timeout(e.get_bool().value);
                }
                break;
//            case MongoFindOption::PROJECTION:
//                options.projection() // bson view or value
            case MongoFindOption::RETURN_KEY:
                if (e.type() == type::k_bool) {
                    options.return_key(e.get_bool().value);
                }
                break;
            case MongoFindOption::SHOW_RECORD_ID:
                if (e.type() == type::k_bool) {
                    options.show_record_id(e.get_bool().value);
                }
                break;
            case MongoFindOption::SKIP:
                if (e.type() == type::k_int32) {
                    options.skip(e.get_int32().value);
                } else if (e.type() == type::k_int64) {
                    options.skip(e.get_int64().value);
                }
                break;
//            case MongoFindOption::SORT:
//                options.sort(e.get_document()); // bson view or value
            default:
                break;
        }
    }
}

int main(int argc, char** argv)
{
    cout << "Agent Mongo" << endl;
    std::string nodename = "cubesat1";
    std::string agentname = "mongo";

    agent = new Agent(nodename, agentname, 1, AGENTMAXBUFFER, false, 20301);

    if (agent->cinfo == nullptr) {
        std::cout << "Unable to start agent_mongo" << std::endl;
        exit(1);
    }

    mongocxx::instance instance {};

    // Connect to a MongoDB URI and establish connection
    mongocxx::client connection {
        mongocxx::uri {
            "mongodb://192.168.150.9:27017/"
        }
    };

    // Create a thread for the data collection and service requests.
    collect_data_thread = thread(collect_data_loop, std::ref(connection));
    service_requests_thread = thread(service_requests, std::ref(connection));

    while(agent->running())
    {
        // Sleep for 1 sec
        COSMOS_SLEEP(0.1);
    }

    agent->shutdown();
    collect_data_thread.join();
    service_requests_thread.join();

    return 0;
}

//! The method to handle incoming telemetry data to write it to the database
/*!
 *
 * \param connection MongoDB connection instance
 */
void collect_data_loop(mongocxx::client &connection)
{
    size_t my_position = static_cast<size_t>(-1);
    while (agent->running())
    {
        // Collect new data
        while (my_position != agent->message_head)
        {
            ++my_position;
            if (my_position >= agent->message_ring.size())
            {
                my_position = 0;
            }

            // Check if type of message is a beat or state of health message
            if (agent->message_ring[my_position].meta.type == Agent::AgentMessage::BEAT || agent->message_ring[my_position].meta.type == Agent::AgentMessage::SOH)
            {
                // First use reference to adata to check conditions
                std::string *padata = &agent->message_ring[my_position].adata;

                // If no content in adata, don't continue or write to database
                if (!padata->empty() && padata->front() == '{' && padata->back() == '}')
                {
                    // Extract the date and name of the node
                    std::string utc = json_extract_namedobject(agent->message_ring[my_position].jdata, "agent_utc");
                    std::string node = json_extract_namedobject(agent->message_ring[my_position].jdata, "agent_node");

                    // Remove leading and trailing quotes around node
                    node.erase(0, 1);
                    node.pop_back();

                    // Connect to the database and store in the collection of the node name
                    auto collection = connection["db"][node]; // store by node

                    bsoncxx::document::view_or_value value;

                    // Copy adata and manipulate string to add the agent_utc (date)
                    std::string adata = agent->message_ring[my_position].adata;
                    adata.pop_back();
                    std::string adata_with_date = adata.append(", \"agent_utc\" : " + utc + "}");

                    // *, exclude, include; command line, request, json file

                    try
                    {
                        // Convert JSON into BSON object to prepare for database insertion
                        value = bsoncxx::from_json(adata_with_date);
                    } catch (const bsoncxx::exception err) {
                        std::cout << "Error converting to BSON from JSON" << std::endl;
                    }

                    try
                    {
                        // Insert BSON object into collection specified
                        auto insert = collection.insert_one(value);

                        std::cout << "Inserted adata into collection " << node << std::endl;
                    } catch (const mongocxx::bulk_write_exception err)
                    {
                        cout << "Error writing to database." << endl;
                    }
                }
            }
        }
        COSMOS_SLEEP(.1);
    }
    return;
}

//! The method to handle incoming requests to query the database
/*!
 *
 * \param connection MongoDB connection instance
 */
void service_requests(mongocxx::client &connection)
{
    while (agent->running())
    {
        char ebuffer[6]="[NOK]";
        int32_t iretn;

        // Check if socket opened•
        if ((iretn = socket_open(&agent->cinfo->agent[0].req, NetworkType::UDP, const_cast<char *>(""), agent->cinfo->agent[0].beat.port, SOCKET_LISTEN, SOCKET_BLOCKING, 2000000)) < 0)
        {
            return;
        }

        // While agent is running
        while (agent->cinfo->agent[0].stateflag)
        {
            char *bufferin, *bufferout;

            // If the socket opened, set the heartbeat port to cport
            agent->cinfo->agent[0].beat.port = agent->cinfo->agent[0].req.cport;

            // Check buffer size

            if ((bufferin = reinterpret_cast<char *> (calloc(1, agent->cinfo->agent[0].beat.bsz))) == NULL)
            {
                iretn = -errno;
                return;
            }

            // Receiving socket data
            iretn = static_cast<int32_t>(recvfrom(
                static_cast<int32_t>(agent->cinfo->agent[0].req.cudp),
                bufferin,
                agent->cinfo->agent[0].beat.bsz,
                0,
                reinterpret_cast<struct sockaddr *>(&agent->cinfo->agent[0].req.caddr),
                reinterpret_cast<socklen_t *>(&agent->cinfo->agent[0].req.addrlen)
            ));

            std::cout << "Receiving " << bufferin << std::endl;

            if (iretn > 0)
            {
                std::cout << "Received request: " << bufferin << std::endl;
                bsoncxx::builder::stream::document document {};

                // Convert the character
                std::string req = bufferin;
                map<std::string, std::string> input = get_keys(req, "?", "=");

                mongocxx::collection collection = connection[input["database"]][input["collection"]];

                std::string response;

                mongocxx::options::find options;

                if (!(input.find("options") == input.end())) {
                    set_mongo_options(options, input["options"]);
                }

                if (input["multiple"] == "true")
                {
                    try
                    {
                        // Query the database based on the filter
                        mongocxx::cursor cursor = collection.find(bsoncxx::from_json(input["query"]), options);

                        // Check if the returned cursor is empty, if so return an empty array
                        if (!(cursor.begin() == cursor.end())) {
                            std::string data;

                            for (auto document : cursor) {
                                data.insert(data.size(), bsoncxx::to_json(document) + ",");
                            }

                            data.pop_back();

                            response = "[" + data + "]";
                        } else if (cursor.begin() == cursor.end() && response.empty()) {
                            response = "[]";
                        }
                    } catch (mongocxx::logic_error err) {
                        std::cout << "Logic error when querying occurred" << std::endl;

                        response = "{\"error\": \"Logic error within the query. Could not query database.\"}";
                    } catch (bsoncxx::exception err) {
                        std::cout << "Could not convert JSON" << std::endl;

                        response = "{\"error\": \"Improper JSON query. Could not convert.\"}";
                    }


                    std::cout << response << std::endl;
                }
                else
                {
                    stdx::optional<bsoncxx::document::value> document;
                    try
                    {
                        document = collection.find_one(bsoncxx::from_json(input["query"]), options);
                    } catch (mongocxx::query_exception err)
                    {
                        std::cout << "Logic error when querying occurred" << std::endl;

                        response = "{\"error\": \"Logic error within the query. Could not query database.\"}";
                    } catch (bsoncxx::exception err)
                    {
                        std::cout << "Could not convert JSON" << std::endl;

                        response = "{\"error\": \"Improper JSON query. Could not convert.\"}";
                    }

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

                if (response.empty())
                {
                    response = ebuffer;
                }

                bufferout = const_cast<char *>(response.c_str());

                sendto(
                    agent->cinfo->agent[0].req.cudp,
                    bufferout,
                    strlen(bufferout),
                    0,
                    reinterpret_cast<struct sockaddr *>(&agent->cinfo->agent[0].req.caddr),
                    *reinterpret_cast<socklen_t *>(&agent->cinfo->agent[0].req.addrlen)
                );

                free(bufferin);
                free(bufferout);
            }
            COSMOS_SLEEP(.1);
        }
    }
}
