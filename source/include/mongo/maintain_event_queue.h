#ifndef MAINTAIN_EVENT_QUEUE_H
#define MAINTAIN_EVENT_QUEUE_H

#include <mongo/agent_mongo.h>

void maintain_event_queue(std::string &agent_list, std::string &shell);

void maintain_event_queue(std::string &agent_list, std::string &shell) {
    while (agent->running()) {

        std::string list;

        list = execute("\"" + agent_list + " neutron1 exec get_event\"", shell);

        try {
            auto json = bsoncxx::from_json(list);
            std::string node_type = "event_queue";

            if (!list.empty()) {
                list.pop_back();

                std::string response = "{\"queue\":\"" + escape_json(list) + "\", \"node_type\": \"event_queue\"}";

                send_live("WS Event Queue", node_type, response);
            }
        } catch (const bsoncxx::exception &err) {
            cout << "WS Event Queue: Error converting to BSON from JSON" << err.what() << endl;
        }

        COSMOS_SLEEP(5.);
    }
}

#endif // MAINTAIN_EVENT_QUEUE_H
