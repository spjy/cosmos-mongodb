#ifndef MAINTAIN_DATA_H
#define MAINTAIN_DATA_H

#include <mongo/agent_mongo.h>
#include <mongo/maintain_agent_list.h>
#include <mongo/maintain_event_queue.h>
#include <mongo/maintain_file_list.h>

void maintain_data(std::string &agent_path, std::string &shell, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string hostnode);

void maintain_data(std::string &agent_path, std::string &shell, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string hostnode) {
    while (agent->running()) {

        // Doesn't require agent exec
        maintain_agent_list();

        beatstruc soh;

        // Check if agent exec is running
        soh = agent->find_agent("any", "exec");
        if (soh.utc == 0.) {
            COSMOS_SLEEP(5.);
            continue;
        }

        // Requires agent exec
        maintain_event_queue(agent_path, shell);
        maintain_file_list(agent_path, shell, included_nodes, excluded_nodes, hostnode);

        COSMOS_SLEEP(5.);
    }
}

#endif // MAINTAIN_DATA_H


