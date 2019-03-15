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
#include <iostream>
#include <fstream>
#include <thread>

#include <support/jsondef.h>
#include "support/jsonlib.h"
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

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
thread cdthread;

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

    mongocxx::database db = client["db"];

    // Initialize the MongoDB session
    auto session = connection.start_session();

    cdthread = thread(collect_data_loop);

    // Start executing the agent
    while(agent->running())
    {
	// while running, listen to the incoming events coming in from satellite
	// store data into the collection, separated by the node coming in from a ring buffer
	// at the same time be able to service requests from the listening client in the thread

        // Specify and connect to a database and collection
        mongocxx::collection collection = db[agent->cinfo->name];

        // Start executing the agent
        //        pos_eci.utc = currentmjd(0);
        //        agent->cinfo->node.loc.pos.eci = pos_eci;
        agent->cinfo->devspec.imu[0]->temp = 123;

        // Convert agent data into JSON string
        string json_data = json_list_of_soh(agent->cinfo);

        // Convert JSON string to BSON document value
        document::value document = bsonxx::from_json(json_data);

        // Insert document into the database
        try {
            mongocxx::collection::insert_one(document);
        } catch (mongocxx::bulk_write_exception err) {
            cout << "Error writing to database." << err << endl;
        }

        //sleep for 1 sec
        COSMOS_SLEEP(0.1);
    }

    agent->shutdown();
    cdthread.join();

    return 0;
}

void collect_data_loop()
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
            if (agent->cinfo->node.name == agent->message_ring[my_position].meta.beat.node && agent->message_ring[my_position].meta.type < Agent::AgentMessage::BINARY)
            {
                json_parse(agent->message_ring[my_position].adata, agent->cinfo);
                agent->cinfo->node.utc = currentmjd(0.);

                for (devicestruc device: agent->cinfo->device)
                {
                    if (device.all.utc > agent->cinfo->node.utc)
                    {
                        agent->cinfo->node.utc = device.all.utc;
                    }
                }
            }
        }
        COSMOS_SLEEP(.1);
    }
    return;
}

// heartbeat / soh msgs
// listening to requests
// sending 

