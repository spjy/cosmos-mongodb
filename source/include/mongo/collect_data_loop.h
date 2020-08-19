#ifndef COLLECT_DATA_LOOP_H
#define COLLECT_DATA_LOOP_H

#include <mongo/agent_mongo.h>

void collect_data_loop(mongocxx::client &connection_live, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &collect_mode, std::string &agent_path, std::string &shell, HttpsClient &client, std::string &token);

void collect_data_loop(mongocxx::client &connection_live, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &collect_mode, std::string &agent_path, std::string &shell, HttpsClient &client, std::string &token)
{
    time_t startTime = time(NULL);
    time_t currentTime;

    if (collect_mode == "agent") {
        while (agent->running()) {
            int32_t iretn;
            time(&currentTime);

            Agent::messstruc message;
            iretn = agent->readring(message, Agent::AgentMessage::ALL, 1., Agent::Where::TAIL);

            if (iretn > 0) {
                // First use reference to adata to check conditions
                std::string *padata = &message.adata;

                if (padata == NULL) {
                    continue;
                }

                // If no content in adata, don't continue or write to database
                if (padata->empty() && padata->front() != '{' && padata->back() != '}') {
                    continue;
                }

                std::string *pjdata = &message.jdata;

                if (pjdata == NULL) {
                    continue;
                }

                // Extract node from jdata
                std::string node = json_extract_namedmember(message.jdata, "agent_node");
                std::string type = json_extract_namedmember(message.jdata, "agent_proc");
                std::string ip = json_extract_namedmember(message.jdata, "agent_addr");

                if (!(node.empty()) && !(type.empty()) && !(ip.empty()))
                {
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
                        auto any_collection = connection_live[realm]["any"];
                        std::string response;
                        mongocxx::options::find options; // store by node

                        bsoncxx::document::view_or_value value;

                        // Copy adata and manipulate string to add the agent_utc (date)
                        std::string adata = message.adata;

                        if (!(adata.empty())) {
                            adata.pop_back();
                            adata.insert(adata.size(), ", \"node_utc\": " + std::to_string(message.meta.beat.utc));
                            adata.insert(adata.size(), ", \"node_type\": \"" + node_type + "\"");
                            adata.insert(adata.size(), ", \"node_ip\": \"" + ip + "\"}");

                            if (token.length() > 10 && difftime(currentTime, startTime) >= 1200) {
                                time(&startTime);
                                std::string json_string = "{\"blocks\":[{\"type\":\"section\",\"text\":{\"type\":\"mrkdwn\",\"text\":\"Activity detected after 30 minutes!\"}},{\"type\":\"divider\"}]}";
                                try {
                                    cout << "POST request to http://hooks.slack.com" << endl;
                                    auto response = client.request("POST", token, json_string);
                                    cout << "Response content: " << response->content.string() << endl;
                                }
                                catch(const SimpleWeb::system_error &e) {
                                    cout << "Client request error: " << e.what() << endl;
                                }
                            }

                            try {
                                // Convert JSON into BSON object to prepare for database insertion
                                value = bsoncxx::from_json(adata);

                                try
                                {
                                    // Insert BSON object into collection specified
                                    live_mtx.lock();
                                    auto any_insert = any_collection.insert_one(value);
                                    live_mtx.unlock();

                                    cout << "WS Live: Inserted adata " << node_type << endl;

                                    if (type != "exec") {
                                        send_live("WS Live", realm, adata);
                                    }
                                } catch (const mongocxx::bulk_write_exception &err) {
                                    cout << "WS Live: " << err.what() << endl;
                                }
                            } catch (const bsoncxx::exception &err) {
                                cout << "WS Live: " << err.what() << endl;
                            }
                        }
                    }
                }
            }
            COSMOS_SLEEP(1.);
        }
    } else if (collect_mode == "soh") {
        while (agent->running()) {
            std::string list;
            time(&currentTime);

            list = execute("\"" + agent_path + " any exec soh\"", shell);

            std::string soh = json_extract_namedmember(list, "output");

            if (!soh.empty()) {
                std::string node = json_extract_namedmember(list, "node_name");
                // If [NOK] is not found, valid response
                if (!node.empty()) {
                    node.erase(0, 1);
                    node.pop_back();

                    if (whitelisted_node(included_nodes, excluded_nodes, node)) {
                        if (token.length() > 10 && difftime(currentTime, startTime) >= 1200) {
                            time(&startTime);
                            std::string json_string = "{\"blocks\":[{\"type\":\"section\",\"text\":{\"type\":\"mrkdwn\",\"text\":\"Activity detected sometime after 30 minutes!\"}},{\"type\":\"divider\"}]}";
                            try {
                                cout << "POST request to http://hooks.slack.com" << endl;
                                auto response = client.request("POST", token, json_string);
                                cout << "Response content: " << response->content.string() << endl;
                            }
                            catch(const SimpleWeb::system_error &e) {
                                cout << "Client request error: " << e.what() << endl;
                            }
                        }

                        auto any_collection = connection_live[realm]["any"];
                        bsoncxx::document::view_or_value value;

                        try {
                            // Convert JSON into BSON object to prepare for database insertion
                            value = bsoncxx::from_json(soh);

                            try
                            {
                                // Insert BSON object into collection specified
                                live_mtx.lock();
                                auto any_insert = any_collection.insert_one(value);
                                live_mtx.unlock();

                                cout << "WS SOH Live: Inserted adata " << node << endl;

                                send_live("WS Live", realm, soh);
                            } catch (const mongocxx::bulk_write_exception &err) {
                                cout << "WS SOH Live: " << err.what() << endl;
                            }
                        } catch (const bsoncxx::exception &err) {
                            cout << "WS SOH Live: " << err.what() << endl;
                        }
                    }
                }
            }
//            beatstruc soh;

//            soh = agent->find_agent("any", "exec");

//            if (soh.utc == 0.) {
//                cout << "Error finding agent exec" << endl;
//            } else {
//                std::string response;
//                int resp = agent->send_request(soh, "soh", response, 3.);

//                std::string node = json_extract_namedmember(response, "node_name");
//                // If [NOK] is not found, valid response
//                if (resp > 0 && response.find("[NOK]") == std::string::npos && !node.empty()) {
//                    node.erase(0, 1);
//                    node.pop_back();

//                    if (whitelisted_node(included_nodes, excluded_nodes, node)) {
//                        cout << response << endl;
//                        auto any_collection = connection_live[realm]["any"];
//                        bsoncxx::document::view_or_value value;

//                        try {
//                            // Convert JSON into BSON object to prepare for database insertion
//                            value = bsoncxx::from_json(response);

//                            try
//                            {
//                                // Insert BSON object into collection specified
//                                live_mtx.lock();
//                                auto any_insert = any_collection.insert_one(value);
//                                live_mtx.unlock();

//                                cout << "WS SOH Live: Inserted adata " << node << endl;

//                                send_live("WS Live", realm, response);
//                            } catch (const mongocxx::bulk_write_exception &err) {
//                                cout << "WS SOH Live: " << err.what() << endl;
//                            }
//                        } catch (const bsoncxx::exception &err) {
//                            cout << "WS SOH Live: " << err.what() << endl;
//                        }
//                    }
//                }

//            }

            COSMOS_SLEEP(5.);
        }
    }

    return;
}

#endif // COLLECT_DATA_LOOP_H
