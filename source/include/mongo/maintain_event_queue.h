#ifndef MAINTAIN_EVENT_QUEUE_H
#define MAINTAIN_EVENT_QUEUE_H

#include <mongo/agent_mongo.h>

void maintain_event_queue(std::string &agent_path, std::string &shell);

void maintain_event_queue(std::string &agent_path, std::string &shell) {
    std::string list;
    beatstruc soh;

    list = execute("\"" + agent_path + " any exec getcommand\"", shell);

    std::string node_type = "event_queue";

    if (!list.empty()) {
        list.pop_back();

        std::string response = "{\"queue\":\"" + escape_json(list) + "\", \"node_type\": \"event_queue\"}";

        send_live("WS Event Queue", node_type, response);
    }
}

#endif // MAINTAIN_EVENT_QUEUE_H
