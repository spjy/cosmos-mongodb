#ifndef PROCESS_FILES_H
#define PROCESS_FILES_H

#include <mongo/agent_mongo.h>

void process_files(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path, std::string agent_type);

//! Walk through the directories created by agent_file and process the telemetry data in the incoming folder.
//! \brief process_files
//! \param connection_file
//! \param database
//! \param included_nodes
//! \param excluded_nodes
//! \param file_walk_path
//! \param agent_type
//!
void process_files(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path, std::string agent_type)
{
    // Get the nodes folder
    fs::path nodes = file_walk_path;

    while (agent->running())
    {
        // Loop through the nodes folder
        for(auto& node: fs::directory_iterator(nodes))
        {
            vector<std::string> node_path = string_split(node.path().string(), "/");

            // Check if node is whitelisted
            if (whitelisted_node(included_nodes, excluded_nodes, node_path.back()))
            {
                fs::path agent = node.path();

                // Get SOH folder
                agent /= "incoming";
                agent /= agent_type;

                // Loop through the folder
                if (is_directory(agent))
                {
                    for (auto& telemetry: fs::directory_iterator(agent))
                    {
                        // only files with JSON structures
                        if (telemetry.path().filename().string().find(".telemetry") != std::string::npos)
                        {
                            // Uncompress telemetry file
                            gzFile gzf = gzopen(telemetry.path().c_str(), "rb");

                            if (gzf == Z_NULL) {
                                cout << "File: Error opening " << telemetry.path().c_str() << endl;
                                // Move the file out of /incoming if we cannot open it
                                std::string corrupt_file = data_base_path(node_path.back(), "corrupt", agent_type, telemetry.path().filename().string());

                                try {
                                    fs::rename(telemetry, corrupt_file);
                                    cout << "File: Moved corrupt file to" << corrupt_file << endl;
                                } catch (const std::error_code &error) {
                                    cout << "File: Could not rename file " << error.message() << endl;
                                }

                                break;
                            }

                            cout << "File: Processing " << telemetry.path().c_str() << endl;

                            // get the file type
                            while (!gzeof(gzf))
                            {
                                std::string line;
                                char *nodeString;
                                char buffer[8192];

                                while (!(line.back() == '\n') && !gzeof(gzf))
                                {
                                    nodeString = gzgets(gzf, buffer, 8192);

                                    if (nodeString == Z_NULL) {
                                        cout << "File: Error getting string " << telemetry.path().c_str() << endl;

                                        break;
                                    }

                                    line.append(buffer);
                                }

                                // Check if it got to end of file in buffer
                                if (!gzeof(gzf) && nodeString != Z_NULL)
                                {
                                    // Get the node's UTC
                                    std::string node_utc = json_extract_namedmember(line, "node_utc");
                                    std::string node_type = node_path.back() + ":" + agent_type;

                                    if (node_utc.length() > 0)
                                    {
                                        auto any_collection = connection_file[realm]["any"];
                                        stdx::optional<bsoncxx::document::value> document;

                                        // Query the database for the node_utc.
                                        try
                                        {
                                            file_mtx.lock();
                                            document = any_collection.find_one(bsoncxx::builder::basic::make_document(kvp("node_utc", stod(node_utc))));
                                            file_mtx.unlock();
                                        }
                                        catch (const mongocxx::query_exception &err)
                                        {
                                            cout << "File: Logic error when querying occurred" << err.what() << endl;
                                        }
                                        catch (const bsoncxx::exception &err)
                                        {
                                            cout << "File: Could not convert JSON" << err.what() << endl;
                                        }

                                        // Append node_type for live values
                                        line.pop_back(); // Rid of \n newline char

                                        // Rid of curly bracket if we pop the newline character
                                        if (line.back() == '}') {
                                            line.pop_back();
                                        }

                                        line.insert(line.size(), ", \"node_type\": \"" + node_type + "\"}");

                                        // If an entry does not exist with node_utc, write the entry into the database
                                        if (!document)
                                        {
                                            bsoncxx::document::view_or_value value;

                                            try
                                            {
                                                // Convert JSON into BSON object to prepare for database insertion
                                                value = bsoncxx::from_json(line);

                                                try
                                                {
                                                    // Insert BSON object into collection specified
                                                    file_mtx.lock();
                                                    auto any_insert = any_collection.insert_one(value);
                                                    file_mtx.unlock();

                                                    send_live("File", node_type, line);
                                                }
                                                catch (const mongocxx::bulk_write_exception &err)
                                                {
                                                    cout << "File: Error writing to database." << err.what() << endl;
                                                }
                                            }
                                            catch (const bsoncxx::exception &err)
                                            {
                                                cout << "File: Error converting to BSON from JSON" << err.what() << endl;
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    break;
                                }
                            }

                            gzclose(gzf);
                        }

                        // Move file to archive
                        std::string archive_file = data_base_path(node_path.back(), "archive", agent_type, telemetry.path().filename().string());

                        try {
                            fs::rename(telemetry, archive_file);

                            cout << "File: Processed file " << telemetry.path() << endl;
                        } catch (const std::error_code &error) {
                            cout << "File: Could not rename file " << error.message() << endl;
                        }
                    }
                }
            }
        }

        cout << "File: Finished walking through files." << endl;

        COSMOS_SLEEP(300);
    }
}
#endif // PROCESS_FILES_H
