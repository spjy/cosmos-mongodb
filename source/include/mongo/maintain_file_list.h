#ifndef MAINTAIN_FILE_LIST_H
#define MAINTAIN_FILE_LIST_H

#include <mongo/agent_mongo.h>

void maintain_file_list(std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &agent_path, std::string &shell, std::string hostnode);

//! The method to handle incoming telemetry data to write it to the database
/*!
 *
 * \param connection MongoDB connection instance
 */
void maintain_file_list(std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &agent_path, std::string &shell, std::string hostnode)
{
    std::string previousFiles;

    while (agent->running())
    {
        std::string list;
        std::string response = "{\"node_type\": \"file\", \"outgoing\": {";

        WsClient client("localhost:8081/live/file_list");

        list = execute("\"" + agent_path + " " + hostnode + " file list_outgoing_json\"", shell);

        if (list.empty()) {
            continue;
        }

        try
        {
            bsoncxx::document::value json = bsoncxx::from_json(list);
            bsoncxx::document::view opt { json.view() };

            bsoncxx::document::element outgoing
            {
                opt["output"].get_document().view()["outgoing"]
            };

            bsoncxx::array::view agent_file_array {outgoing.get_array().value};

            for (bsoncxx::array::element e : agent_file_array)
            {
                if (e)
                {
                    bsoncxx::document::element node
                    {
                        e.get_document().view()["node"]
                    };

                    bsoncxx::document::element count
                    {
                        e.get_document().view()["count"]
                    };

                    bsoncxx::document::element files
                    {
                        e.get_document().view()["files"]
                    };

                    std::string node_string = bsoncxx::string::to_string(node.get_utf8().value);

                    if (count.get_int32().value != 0 && whitelisted_node(included_nodes, excluded_nodes, node_string))
                    {
                        response.insert(response.size(), "\"" + bsoncxx::string::to_string(node.get_utf8().value) + "\":[");

                        bsoncxx::array::view node_files {files.get_array().value};

                        for (bsoncxx::array::element e : node_files)
                        {
                            if (e)
                            {
                                bsoncxx::document::element tx_id
                                {
                                    e.get_document().view()["tx_id"]
                                };

                                bsoncxx::document::element agent
                                {
                                    e.get_document().view()["agent"]
                                };

                                bsoncxx::document::element name
                                {
                                    e.get_document().view()["name"]
                                };

                                bsoncxx::document::element size
                                {
                                    e.get_document().view()["size"]
                                };

                                bsoncxx::document::element bytes
                                {
                                    e.get_document().view()["bytes"]
                                };

                                response.insert(response.size(),
                                    "{\"tx_id\":" + std::to_string(tx_id.get_int32().value) +
                                    ",\"agent\":\"" + bsoncxx::string::to_string(agent.get_utf8().value) + "\"" +
                                    ",\"name\":\"" + bsoncxx::string::to_string(name.get_utf8().value) + "\"" +
                                    ",\"size\":" + std::to_string(size.get_int32().value) +
                                    ",\"bytes\":" + std::to_string(bytes.get_int32().value) +
                                    "},");
                            }
                        }

                        if (response.back() == ',')
                        {
                            response.pop_back();
                        }

                        response.insert(response.size(), "],");
                    }
                }
            }
        }
        catch (const bsoncxx::exception &err)
        {
            cout << "WS File Live: " << err.what() << endl;
        }

        if (response.back() == ',')
        {
            response.pop_back();
        }

        response.insert(response.size(), "}, \"incoming\": {");

        list = execute("\"" + agent_path + " " + hostnode + " file list_incoming_json\"", shell);

        try
        {
            bsoncxx::document::value json = bsoncxx::from_json(list);
            bsoncxx::document::view opt { json.view() };

            bsoncxx::document::element incoming
            {
                opt["output"].get_document().view()["incoming"]
            };

            bsoncxx::array::view agent_file_array {incoming.get_array().value};

            for (bsoncxx::array::element e : agent_file_array)
            {
                if (e)
                {
                    bsoncxx::document::element node
                    {
                        e.get_document().view()["node"]
                    };

                    bsoncxx::document::element count
                    {
                        e.get_document().view()["count"]
                    };

                    bsoncxx::document::element files
                    {
                        e.get_document().view()["files"]
                    };

                    std::string node_string = bsoncxx::string::to_string(node.get_utf8().value);

                    if (count.get_int32().value != 0 && whitelisted_node(included_nodes, excluded_nodes, node_string))
                    {
                        response.insert(response.size(), "\"" + bsoncxx::string::to_string(node.get_utf8().value) + "\":[");

                        bsoncxx::array::view node_files {files.get_array().value};

                        for (bsoncxx::array::element e : node_files)
                        {
                            if (e)
                            {
                                bsoncxx::document::element tx_id
                                {
                                    e.get_document().view()["tx_id"]
                                };

                                bsoncxx::document::element agent
                                {
                                    e.get_document().view()["agent"]
                                };

                                bsoncxx::document::element name
                                {
                                    e.get_document().view()["name"]
                                };

                                bsoncxx::document::element size
                                {
                                    e.get_document().view()["size"]
                                };

                                bsoncxx::document::element bytes
                                {
                                    e.get_document().view()["bytes"]
                                };

                                response.insert(response.size(),
                                    "{\"tx_id\":" + std::to_string(tx_id.get_int32().value) +
                                    ",\"agent\":\"" + bsoncxx::string::to_string(agent.get_utf8().value) + "\"" +
                                    ",\"name\":\"" + bsoncxx::string::to_string(name.get_utf8().value) + "\"" +
                                    ",\"size\":" + std::to_string(size.get_int32().value) +
                                    ",\"bytes\":" + std::to_string(bytes.get_int32().value) +
                                    "},");
                            }
                        }

                        if (response.back() == ',')
                        {
                            response.pop_back();
                        }

                        response.insert(response.size(), "],");
                    }
                }
            }
        }
        catch (const bsoncxx::exception &err)
        {
            cout << "WS File Live: " << err.what() << endl;
        }

        if (response.back() == ',')
        {
            response.pop_back();
        }

        response.insert(response.size(), "}}");

        client.on_open = [&response](std::shared_ptr<WsClient::Connection> connection)
        {
            cout << "WS File Live: Sending message" << endl;
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

        previousFiles = response;

        COSMOS_SLEEP(5);
    }
}

#endif // MAINTAIN_FILE_LIST_H
