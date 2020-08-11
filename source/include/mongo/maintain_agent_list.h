#ifndef MAINTAIN_AGENT_LIST_H
#define MAINTAIN_AGENT_LIST_H

#include <mongo/agent_mongo.h>

void maintain_agent_list();

//! Maintain a list of agents and send it through the socket.
//! \brief maintain_agent_list Query the agent list at a certain interval and maintain the list in a sorted set. Send it off to the websocket if anything changes.
//! Execute the agent list_json command, check if node is whitelisted, extract data from json, insert into set, if set is changed then send the update via the live websocket.
//!
void maintain_agent_list()
{
    std::map<std::string, std::string> fullList;

    while (agent->running())
    {
        std::set<std::pair<std::string, std::string>> sortedAgents;
        std::string list;

        for (beatstruc agent : agent->agent_list) {
            std::string node = agent.node;
            std::string proc = agent.proc;
            std::string utc = std::to_string(agent.utc);

            std::pair<std::string, std::string> agent_node_proc_utc(node + ":" + proc, utc);

            sortedAgents.insert(agent_node_proc_utc);
        }

        std::string response = "{\"node_type\": \"list\", \"agent_list\": [";

        std::for_each(sortedAgents.begin(), sortedAgents.end(), [&fullList, &response](const std::pair<std::string, std::string> &item)
        {
            std::string past_utc = fullList[std::get<0>(item)];

            if (past_utc == '-' + std::get<1>(item)) {
                response.insert(response.size(), "{\"agent\": \"" + std::get<0>(item) + "\", \"utc\": -" + std::get<1>(item) + "},");
            } else if (past_utc == std::get<1>(item)) {
                response.insert(response.size(), "{\"agent\": \"" + std::get<0>(item) + "\", \"utc\": " + std::get<1>(item) + "},");
                fullList[std::get<0>(item)] = '-' + std::get<1>(item);
            } else {
                response.insert(response.size(), "{\"agent\": \"" + std::get<0>(item) + "\", \"utc\": " + std::get<1>(item) + "},");
                fullList[std::get<0>(item)] = std::get<1>(item);
            }
        });

        if (response.back() == ',')
        {
            response.pop_back();
        }

        response.insert(response.size(), "]}");

        WsClient client("localhost:8081/live/list");

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

        COSMOS_SLEEP(5);
    }
 }
#endif // MAINTAIN_AGENT_LIST_H
