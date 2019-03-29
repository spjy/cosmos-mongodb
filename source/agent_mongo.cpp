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

Agent *agent;
std::ofstream file;

void collect_data_loop(mongocxx::client &connection);
void service_requests(mongocxx::client &connection);
map<std::string, std::string> get_keys(std::string &request, const std::string variable_delimiter, const std::string value_delimiter);

std::thread collect_data_thread;
std::thread service_requests_thread;

// database=db?variable=value?variable=value

map<std::string, std::string> get_keys(std::string &request, const std::string variable_delimiter, const std::string value_delimiter) {
    // Delimit by key value pairs
    vector<std::string> input = string_split(request, variable_delimiter);

    map<std::string, std::string> keys;

    // Delimit
    for (vector<std::string>::iterator it = input.begin(); it != input.end(); ++it) {
        vector<std::string> kv = string_split(*it, value_delimiter);

        keys[kv[0]] = kv[1]; // Set the variable to the map key and assign the corresponding value
    }

    return keys;
}

enum class MongoFindOption {
    BATCH_SIZE,
    COLLATION,
    COMMENT,
    CURSOR_TYPE,
    HINT,
    LIMIT,
    MAX,
    MAX_AWAIT_TIME,
    MAX_SCAN,
    MAX_TIME,
    MIN,
    NO_CURSOR_TIMEOUT,
    PROJECTION,
    READ_PREFERENCE,
    MODIFIERS,
    RETURN_KEY,
    SHOW_RECORD_ID,
    SKIP,
    SNAPSHOT,
    SORT
};


void str_to_lowercase(std::string &input) {
    std::locale locale;
    for (std::string::size_type i = 0; i < input.length(); ++i) {
        std::tolower(input[i], locale);
    }
}

MongoFindOption option_table(std::string input) {
    str_to_lowercase(input);

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
}

void set_mongo_options(mongocxx::options::find &options, std::string request) {
    bsoncxx::document::value reqJSON = bsoncxx::from_json(request);
    bsoncxx::document::view opt { reqJSON.view() };

    for (auto e : opt) {
        std::string key = std::string(e.key());

        stdx::string_view field_key{e.key()};

        MongoFindOption option = option_table(key);

        switch(option) {
            case MongoFindOption::BATCH_SIZE:
                if (e.type() == type::k_bool) {
                    options.allow_partial_results(e.get_bool().value);
                }
                break;
//            case MongoFindOption::COLLATION:
//                options.batch_size(e.get_int32().value); // string view or value
//                break;
            case MongoFindOption::LIMIT:
                if (e.type() == type::k_int32) {
                    options.limit(e.get_int32().value);
                } else if (e.type() == type::k_int64) {
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
    std::string agentname = "mongo"; //name of the agent that the request is directed to

    agent = new Agent(nodename, agentname, 1, AGENTMAXBUFFER, false, 20301);

    if (agent->cinfo == nullptr) {
        std::cout << "Unable to start agent_mongo" << std::endl;
        exit(1);
    }

    // add state of health (soh)
    std::string soh = "{\"device_batt_current_000\"}";

    // examples
    // SOH for IMU temperature
    soh = "{\"device_imu_temp_000\"}";

    // SOH for location information (utc, position in ECI, attitude in ICRF)
    soh = "{\"node_loc_utc\","
          "\"node_loc_pos_eci\","
          "\"node_loc_att_icrf\"}" ;

    // TODO: create append function for soh to make it easier to add strings
    // example: soh.append("node_loc_pos_eci");

    // set the soh string
    agent->set_sohstring(soh.c_str());
    // Create a MongoDB instance

    mongocxx::instance instance {};

    // Connect to a MongoDB URI
    mongocxx::client connection {
        mongocxx::uri {
            "mongodb://192.168.150.9:27017/"
        }
    };

    // Initialize the MongoDB session

    collect_data_thread = thread(collect_data_loop, std::ref(connection));
    service_requests_thread = thread(service_requests, std::ref(connection));

    // Start executing the agent
    while(agent->running())
    {

        // while running, listen to the incoming events coming in from satellite
        // store data into the collection, separated by the node coming in from a ring buffer
        // at the same time be able to service requests from the listening client in the thread

        // Specify and connect to a database and collection

        // Start executing the agent
        //        pos_eci.utc = currentmjd(0);
        //        agent->cinfo->node.loc.pos.eci = pos_eci;
        agent->cinfo->devspec.imu[0]->temp = 123;

        //sleep for 1 sec
        COSMOS_SLEEP(0.1);
    }

    agent->shutdown();
    collect_data_thread.join();
    service_requests_thread.join();

    return 0;
}

void collect_data_loop(mongocxx::client &connection)
{
    size_t my_position = -1;
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

            // if (agent->message_ring[my_position].meta.type < Agent::AgentMessage::BINARY) // collect heartbeat            //
            // Check if type is a heartbeat and if the type is less than binary
            if (agent->message_ring[my_position].meta.type == Agent::AgentMessage::BEAT || agent->message_ring[my_position].meta.type == Agent::AgentMessage::SOH)
            {
                // First use reference to adata to check conditions
                std::string *padata = &agent->message_ring[my_position].adata;

                // If no content in adata, don't continue or write to database
                if (!padata->empty() && padata->front() == '{' && padata->back() == '}') {
                    // Extract the name of the node
                    std::string utc = json_extract_namedobject(agent->message_ring[my_position].jdata, "agent_utc");
                    std::string node = json_extract_namedobject(agent->message_ring[my_position].jdata, "agent_node");

                    node.erase(0, 1);
                    node.pop_back();

                    // Connect to the database and store in the collection of the node name
                    auto collection = connection["db"][node]; // store by node

                    bsoncxx::document::view_or_value value;

                    // Copy adata and manipulate string to add the agent_utc (date)
                    std::string adata = agent->message_ring[my_position].adata;
                    adata.pop_back();

                    std::string adata_with_date = adata.append(", \"agent_utc\" : " + utc + "}");

                    try {
                        // Convert JSON into BSON object to prepare for database insertion
                        value = bsoncxx::from_json(adata_with_date);
                    } catch (const bsoncxx::exception err) {
                        std::cout << "Error converting to BSON from JSON" << std::endl;
                    }

                    try {
                        // Insert BSON object into collection specified
                        auto insert = collection.insert_one(value);

                        std::cout << "Inserted adata into collection " << node << std::endl;
                    } catch (const mongocxx::bulk_write_exception err) {
                        cout << "Error writing to database." << endl;
                    }
                }
            }
        }
        COSMOS_SLEEP(.1);
    }
    return;
}

void service_requests(mongocxx::client &connection) {
    while (agent->running()) {
        char ebuffer[6]="[NOK]";
        int32_t iretn, nbytes;

        char request[AGENTMAXBUFFER + 1];
        uint32_t i;

        // Check if socket opened•••••••••
        if ((iretn = socket_open(&agent->cinfo->agent[0].req, NetworkType::UDP, (char *)"", agent->cinfo->agent[0].beat.port, SOCKET_LISTEN, SOCKET_BLOCKING, 2000000)) < 0)
        {
            return;
        }

        // While agent is running
        while (agent->cinfo->agent[0].stateflag)
        {
            char *bufferin, *bufferout, *empty = "";

            // If the socket opened, set the heartbeat port to cport
            agent->cinfo->agent[0].beat.port = agent->cinfo->agent[0].req.cport;

            // Check buffer size

            if ((bufferin = (char *) calloc(1, agent->cinfo->agent[0].beat.bsz)) == NULL)
            {
                iretn = -errno;
                return;
            }

            // Receiving socket data
            iretn = recvfrom(
                agent->cinfo->agent[0].req.cudp,
                bufferin,
                agent->cinfo->agent[0].beat.bsz,
                0,
                (struct sockaddr *)&agent->cinfo->agent[0].req.caddr,
                (socklen_t *)&agent->cinfo->agent[0].req.addrlen
            );

            std::cout << "Receiving " << bufferin << std::endl;

            if (iretn > 0)
            {
                std::cout << "Received request: " << bufferin << std::endl;
                bsoncxx::builder::stream::document document {};
                // variable=value?variable=value?variable=value
                // database=db?collection=agent?filter={"temperature":"75","acceleration":"2"}

                // possible variables to pass
                // database, collection, query, options (differ between find_one and find)
                // delimit first by question marks, then convert each key value pair into a map
                // then for the filter, permanent simulation of neutron1

                // Convert the character
                std::string req = bufferin;

                map<std::string, std::string> input = get_keys(req, "?", "=");

                mongocxx::collection collection = connection[input["database"]][input["collection"]];

                std::string response;

                mongocxx::options::find options;

                set_mongo_options(options, input["options"]);

                if (input["multiple"] == "true")
                {
                    try {
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
                        } else {
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
                    try {
                        document = collection.find_one(bsoncxx::from_json(input["query"]), options);
                    } catch (mongocxx::query_exception err) {
                        std::cout << "Logic error when querying occurred" << std::endl;

                        response = "{\"error\": \"Logic error within the query. Could not query database.\"}";
                    } catch (bsoncxx::exception err) {
                        std::cout << "Could not convert JSON" << std::endl;

                        response = "{\"error\": \"Improper JSON query. Could not convert.\"}";
                    }

                    // Check if document is empty, if so return an empty object
                    if (document) {
                        std::string data;

                        data = bsoncxx::to_json(document.value());

                        response = data;

                    } else {
                        response = "{}";
                    }
                }

                if (response.empty()) {
                    response = ebuffer;
                }

                bufferout = (char *)response.c_str();
                sendto(agent->cinfo->agent[0].req.cudp, bufferout, strlen(bufferout), 0, (struct sockaddr *)&agent->cinfo->agent[0].req.caddr, *(socklen_t *)&agent->cinfo->agent[0].req.addrlen);
            }
            COSMOS_SLEEP(.1);
            free(bufferin);
        }
    }
}
