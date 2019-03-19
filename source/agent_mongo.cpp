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
#include "support/socket.h"
#include "support/stringlib.h"
#include "support/jsondef.h"
#include "support/jsonlib.h"
#include <iostream>
#include <fstream>
#include <thread>

#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#define ADDRESS "225.1.1.1"
#define PORT 10020

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;
using namespace mongocxx;
using bsoncxx::builder::basic::kvp;

using namespace std;

Agent *agent;
ofstream file;
void collect_data_loop();
thread collect_data_thread;
thread service_requests_thread;

// variable=value?variable=value?variable=value

map<string, string> get_map(string request) {
    // Delimit by key value pairs
    vector<string> input = string_split(request, "?");

    map<string, string> keys;

    // Delimit
    for (vector<string>::iterator it = input.begin(); it != input.end(); ++it) {
        vector<string> kv = string_split(*it, "=");

        keys[kv[0]] = kv[1]; // Set the variable to the map key and assign the corresponding value
    }

    if (!(keys.find("database") == keys.end()
            && keys.find("collection") == keys.end()
            && keys.find("filter") == keys.end())) {
        throw "Not all required fields were provided.";
    }

    return keys;
};

int main(int argc, char** argv)
{
    cout << "Agent Template" << endl;
    Agent *agent;
    string nodename = "cubesat1";
    string agentname = "mongodb"; //name of the agent that the request is directed to

    agent = new Agent(nodename, agentname);

    // add state of health (soh)
    string soh = "{\"device_batt_current_000\"}";

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
            "mongodb://localhost"
        }
    };

    // Initialize the MongoDB session
    auto session = connection.start_session();

    collect_data_thread = thread(collect_data_loop(database));
    service_requests_thread = thread(service_requests(database));

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
connection
    agent->shutdown();
    collect_data_thread.join();
    service_requests_thread.join();

    return 0;
}

void collect_data_loop(mongocxx::client connection)
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
                // Get collection of name agent
                auto collection = connection["db"][agent->getAgent()];

                // Convert agent data into JSON string
                string json_data = json_list_of_soh(agent->cinfo);

                // Convert JSON string to BSON document value
                document::value document = bsonxx::from_json(json_data);

                // Insert document into the database receivedData
                try {
                    auto insert = collection.insert_one(document);
                } catch (mongocxx::bulk_write_exception err) {
                    cout << "Error writing to database." << err << endl;
                }
            }
        }
        COSMOS_SLEEP(.1);
    }
    return;
}

void service_requests(mongocxx::client connection) {
    while (agent->running()) {
        char ebuffer[6]="[NOK]";
        int32_t iretn, nbytes;
        char *bufferin, *bufferout;
        char request[AGENTMAXBUFFER + 1];
        uint32_t i;

        // Check if socket opened
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

            if (iretn > 0)
            {
                bsoncxx::builder::stream::document document{} ;
                // variable=value?variable=value?variable=value
                // database=db?collection=agent?filter={"temperature":"75","acceleration":"2"}

                // clean out any spaces
                // delimit first by question marks, then convert each key value pair into a map
                // then for the filter, permanent simulation of neutron1

                // Convert the character
                string buffer = bufferin;

                map<string, string> input = get_map(buffer);

                mongocxx::collection collection = connection[input["database"]][input["collection"]];
                auto cursor = collection.find();

            }
        }
        free(bufferin);
    }
}

// heartbeat / soh msgs
// listening to requests
// sending
