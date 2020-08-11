#ifndef PROCESS_COMMANDS_H
#define PROCESS_COMMANDS_H

#include <mongo/agent_mongo.h>

void process_commands(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path, std::string agent_type);

//!
//! \brief process_commands
//! \param connection_file
//! \param database
//! \param included_nodes
//! \param excluded_nodes
//! \param file_walk_path
//! \param agent_type
//!
void process_commands(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path, std::string agent_type)
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
                    for (auto& telemetry: fs::directory_iterator(agent)) {
                        // only files with JSON structures
                        if (telemetry.path().filename().string().find(".event") != std::string::npos)
                        {
                            // Uncompress telemetry file
                            gzFile gzf = gzopen(telemetry.path().c_str(), "rb");

                            // Get corresponding .out file of telemetry file
                            fs::path outFile = telemetry.path().parent_path().string() + "/" + telemetry.path().stem().stem().string() + ".out.gz";
                            gzFile out = gzopen(outFile.c_str(), "rb");

                            // Check if valid file
                            if (gzf == Z_NULL || out == Z_NULL) {
                                cout << "File: Error opening " << telemetry.path().c_str() << endl;
                                // Move the file out of /incoming if we cannot open it
                                std::string corrupt_file = data_base_path(node_path.back(), "corrupt", agent_type, telemetry.path().filename().string());
                                std::string corrupt_file_out = data_base_path(node_path.back(), "corrupt", agent_type, outFile.filename().string());

                                try {
                                    fs::rename(telemetry, corrupt_file);

                                    if (fs::is_regular_file(outFile)) {
                                        fs::rename(outFile, corrupt_file_out);
                                    }
                                    cout << "File: Moved corrupt file to" << corrupt_file << endl;
                                    cout << "File: Moved corrupt out file to" << corrupt_file_out << endl;
                                } catch (const std::error_code &error) {
                                    cout << "File: Could not rename file " << error.message() << endl;
                                }

                                break;
                            }

                            cout << "File: Processing " << telemetry.path().c_str() << endl;
                            cout << "File: Processing " << outFile << endl;

                            // get the file type
                            while (!gzeof(gzf))
                            {
                                std::string line;
                                char *nodeString;
                                char buffer[8192];

                                while (!(line.back() == '\n') && !gzeof(gzf))
                                {
                                    int errornum;
                                    nodeString = gzgets(gzf, buffer, 8192);

                                    if (nodeString == Z_NULL) {
                                        cout << "File: Error getting string " << telemetry.path().c_str() << endl;

                                        cout << gzerror(gzf, &errornum);

                                        break;
                                    }

                                    line.append(buffer);
                                }

                                // Check if it got to end of file in buffer
                                if (!gzeof(gzf) && nodeString != Z_NULL)
                                {
                                    // Get the node's UTC
                                    double event_utc = stod(json_extract_namedmember(line, "event_utc"));
                                    std::string event_name = json_extract_namedmember(line, "event_name");

                                    // Remove quotes
                                    event_name.erase(0, 1);
                                    event_name.pop_back();

                                    if (event_name.length() > 0)
                                    {
                                        auto collection = connection_file[realm][node_path.back() + ":executed"];
                                        stdx::optional<bsoncxx::document::value> document;

                                        // Query the database for the utc and name, then replace to add the event_utcexec
                                        try
                                        {
                                            bsoncxx::builder::basic::document basic_builder{};
                                            basic_builder.append(kvp("event_utc", event_utc));
                                            basic_builder.append(kvp("event_name", event_name));
                                            bsoncxx::document::view_or_value query = basic_builder.extract();

                                            std::string line_out;

                                            while (!gzeof(out))
                                            {
                                                char out_buffer[8192];
                                                int32_t iretn = gzread(out, out_buffer, 8192);

                                                if (iretn > 0) {
                                                    line_out.append(out_buffer);
                                                } else {
                                                    cout << "Error reading out file" << outFile.c_str() << endl;

                                                    break;
                                                }
                                            }

                                            // Append node_type for live values
                                            line.pop_back(); // Rid of \n newline char

                                            // Rid of curly bracket if we pop the newline character
                                            if (line.back() == '}') {
                                                line.pop_back();
                                            }

                                            line.insert(line.size(), ", \"output\": \"" + escape_json(line_out) + "\"}");

                                            std::string type = "event";

                                            std::string node_type = node_path.back() + ":executed";
                                            send_live("File", node_type, line);

                                            stdx::optional<bsoncxx::document::value> document = collection.find_one_and_replace(query, bsoncxx::from_json(line));

                                            if (!document) {
                                                command_mtx.lock();
                                                collection.insert_one(bsoncxx::from_json(line));
                                                command_mtx.unlock();
                                            }

                                            gzclose(out);
                                        }
                                        catch (const mongocxx::query_exception &err)
                                        {
                                            cout << "File: Logic error when querying occurred" << err.what() << endl;
                                        }
                                        catch (const bsoncxx::exception &err)
                                        {
                                            cout << "File: Could not convert JSON" << err.what() << endl;
                                        }
                                    }
                                }
                                else
                                {
                                    break;
                                }
                            }

                            gzclose(gzf);

                            // Move file to archive
                            std::string archive_file = data_base_path(node_path.back(), "archive", agent_type, telemetry.path().filename().string());
                            std::string archive_file_out = data_base_path(node_path.back(), "archive", agent_type, outFile.filename().string());

                            try {
                                fs::rename(telemetry, archive_file);

                                if (fs::is_regular_file(outFile)) {
                                    fs::rename(outFile, archive_file_out);
                                }

                                cout << "File: Processed file " << telemetry.path().c_str() << endl;
                                cout << "File: Processed file " << outFile.c_str() << endl;
                            } catch (const std::error_code &error) {
                                cout << "File: Could not rename file " << error.message() << endl;
                            }
                        }
                    }
                }
            }
        }

        cout << "File: Finished walking through files." << endl;

        COSMOS_SLEEP(5);
    }
}
#endif // PROCESS_COMMANDS_H
