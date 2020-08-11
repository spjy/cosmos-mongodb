#ifndef MAINTAIN_EVENT_QUEUE_H
#define MAINTAIN_EVENT_QUEUE_H

#include <mongo/agent_mongo.h>

void maintain_event_queue(std::string &agent_list, std::string &shell);

void maintain_event_queue(std::string &agent_list, std::string &shell) {
    while (agent->running()) {

        std::string list;

        list = execute("\"" + agent_list + " neutron1 exec get_event\"", shell);

        std::string node_type = "event_queue";

        if (!list.empty()) {
            send_live("WS Event Queue", node_type, list);
        }

        COSMOS_SLEEP(5.);
    }
}

#endif // MAINTAIN_EVENT_QUEUE_H
