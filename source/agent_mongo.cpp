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
#include <iostream>
#include <fstream>
#include <thread>
#include <string>
#include <vector>
#include <map>

#include <bsoncxx/array/element.hpp>
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/exception/error_code.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/cursor.hpp>

using namespace bsoncxx;
using bsoncxx::builder::basic::kvp;

Agent *agent;
std::ofstream file;

void collect_data_loop(mongocxx::client &connection);
void service_requests(mongocxx::client &connection);
map<std::string, std::string> get_keys(std::string &request, std::string variable_delimiter, std::string value_delimiter);

std::thread collect_data_thread;
std::thread service_requests_thread;

// database=db?variable=value?variable=value

map<std::string, std::string> get_keys(std::string &request, std::string variable_delimiter, std::string value_delimiter) {
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

void post_database_data(document::view cursor) {
    std::string data = bsoncxx::to_json(cursor);

    agent->post(Agent::AgentMessage::SOH, data);
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
                std::string adata = agent->message_ring[my_position].adata;

                // If no content in adata, don't continue or write to database
                if (!adata.empty() && adata[0] == '{' && adata[adata.size() - 1] == '}') {
                    // Extract the name of the node
                    std::string utc = json_extract_namedobject(agent->message_ring[my_position].jdata, "agent_utc");
                    std::string node = json_extract_namedobject(agent->message_ring[my_position].jdata, "agent_node");

                    node.erase(0, 1);
                    node.pop_back();

                    // Connect to the database and store in the collection of the node name
                    auto collection = connection["db"][node]; // store by node

                    bsoncxx::builder::basic::document builder {};

                    builder.append(kvp("agent_utc", utc));
                    bsoncxx::document::view_or_value value;

                    try {
                        // Convert JSON into BSON object to prepare for database insertion
                        value = bsoncxx::from_json(agent->message_ring[my_position].adata);
                    } catch (bsoncxx::exception err) {
                        std::cout << "Error converting to BSON from JSON" << std::endl;

                        throw err;
                    }

                    try {
                        // Insert BSON object into collection specified
                        auto insert = collection.insert_one(value);

                        std::cout << "Inserted adata into collection " << node << std::endl;
                    } catch (mongocxx::bulk_write_exception err) {
                        cout << "Error writing to database." << endl;

                        throw err;
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
        char *bufferin, *bufferout;
        char request[AGENTMAXBUFFER + 1];
        uint32_t i;

        // Check if socket opened•••••••••
        if ((iretn = socket_open(&agent->cinfo->agent[0].req, NetworkType::UDP, (char *)"", agent->cinfo->agent[0].beat.port, SOCKET_LISTEN, SOCKET_BLOCKING, 2000000)) < 0)
        {
            return;
        }

        // If the socket opened, set the heartbeat port to cport
        agent->cinfo->agent[0].beat.port = agent->cinfo->agent[0].req.cport;

        // Check buffer size

        if ((bufferin = (char *) calloc(1, agent->cinfo->agent[0].beat.bsz)) == NULL)
        {
            iretn = -errno;
            return;
        }

        // While agent is running
        while (agent->cinfo->agent[0].stateflag)
        {
            // Receiving socket data
            iretn = recvfrom(
                agent->cinfo->agent[0].req.cudp,
                bufferin,
                agent->cinfo->agent[0].beat.bsz,
                0,
                (struct sockaddr *)&agent->cinfo->agent[0].req.caddr,
                (socklen_t *)&agent->cinfo->agent[0].req.addrlen
            );


            std::cout << "Hi, I am ready to receive. " << bufferin << std::endl;
            agent->post(Agent::AgentMessage::SOH, "");

            if (iretn > 0)
            {
                std::cout << "Hi, I am receiving data. " << bufferin << std::endl;
                bsoncxx::builder::stream::document document {};
                // variable=value?variable=value?variable=value
                // database=db?collection=agent?filter={"temperature":"75","acceleration":"2"}

                // possible variables to pass
                // database, collection, query, multiple (differ between find_one and find)
                // delimit first by question marks, then convert each key value pair into a map
                // then for the filter, permanent simulation of neutron1

                // Convert the character
                std::string request = bufferin;

                map<std::string, std::string> input = get_keys(request, "?", "=");

                mongocxx::collection collection = connection[input["database"]][input["collection"]];

                std::cout << input["database"] << input["collection"] << std::endl;

                if (input["multiple"] == "true") {
                    mongocxx::cursor cursor = collection.find({});

                    std::string data;

                    for (auto document : cursor) {
                        data = bsoncxx::to_json(document);
                        std::cout << bsoncxx::to_json(document) << "\n";
                    }
                    agent->post(Agent::AgentMessage::SOH, "multiple" + data);
                } else {
                    stdx::optional<bsoncxx::document::value> document = collection.find_one(bsoncxx::from_json(input["query"]));

                    std::string data;

                    data = bsoncxx::to_json(document.value());

                    agent->post(Agent::AgentMessage::SOH, "single" +data);
                }
            }
            COSMOS_SLEEP(.1);
        }
        free(bufferin);
    }
}
