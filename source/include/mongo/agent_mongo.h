#ifndef AGENT_MONGO_H
#define AGENT_MONGO_H

/**
  * Contains definitions for agent mongo.
  */

#include "agent/agentclass.h"
#include "support/configCosmos.h"
#include "agent/agentclass.h"
#include "device/serial/serialclass.h"
#include "support/socketlib.h"
#include "support/stringlib.h"
#include "support/jsondef.h"
#include "support/jsonlib.h"
#include "support/datalib.h"

#include <cstdlib>
#include <string.h>
#include <cstring>
#include <ctime>
#include <iterator>
#include <iostream>
#include <fstream>
#include <thread>
#include <string>
#include <vector>
#include <map>
#include <locale>
#include <memory>
#include <cstdio>
#include <stdexcept>
#include <array>
#include <experimental/filesystem>
#include <set>
#include <tuple>
#include <sstream>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/types/value.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/pool.hpp>

#include <uWebSockets/App.h>
#include <pistache/endpoint.h>

using namespace Pistache;

// #include <server_ws.hpp>
// #include <client_ws.hpp>
// #include <server_http.hpp>
// #include <client_https.hpp>

// using WsServer = SimpleWeb::SocketServer<SimpleWeb::WS>;
// using WsClient = SimpleWeb::SocketClient<SimpleWeb::WS>;

// using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
// using HttpsClient = SimpleWeb::Client<SimpleWeb::HTTPS>;

using namespace bsoncxx;
using bsoncxx::builder::basic::kvp;
using namespace bsoncxx::builder::stream;
namespace fs = std::experimental::filesystem;

std::mutex live_mtx{};
std::mutex query_mtx{};
std::mutex file_mtx{};
std::mutex command_mtx{};

static Agent *agent;

//! Options available to specify when querying a Mongo database
enum class MongoFindOption
{
    //! If some shards are unavailable, it returns partial results if true.
    ALLOW_PARTIAL_RESULTS,
    //! The number of documents to return in the first batch.
    BATCH_SIZE,
    //! Specify language specific rules for string comparison.
    COLLATION,
    //! Comment to attach to query to assist in debugging
    COMMENT,
    //! The cursor type
    CURSOR_TYPE,
    //! Specify index name
    HINT,
    //! The limit of how many documents you retrieve.
    LIMIT,
    //! Get the upper bound for an index.
    MAX,
    //! Max time for the server to wait on new documents to satisfy cursor query
    MAX_AWAIT_TIME,
    //! Deprecated
    MAX_SCAN,
    //! Max time for the oepration to run in milliseconds on the server
    MAX_TIME,
    //! Inclusive lower bound for index
    MIN,
    //! Prevent cursor from timing out server side due to activity.
    NO_CURSOR_TIMEOUT,
    //! Projection which limits the returned fields for the matching documents
    PROJECTION,
    //! Read preference
    READ_PREFERENCE,
    //! Deprecated
    MODIFIERS,
    //! Deprecated
    RETURN_KEY,
    //! Whether to include record identifier in results
    SHOW_RECORD_ID,
    //! Specify the number of documents to skip when querying
    SKIP,
    //! Deprecated
    SNAPSHOT,
    //! Order to return the matching documents.
    SORT,
    INVALID
};

const std::string device_type[] = {"pload", "ssen", "imu", "rw", "mtr", "cpu", "gps", "ant", "rxr", "txr", "tcv", "pvstrg", "batt", "htr", "motr", "tsen", "thst", "prop", "swch", "rot", "stt", "mcc", "tcu", "bus", "psen", "suchi", "cam", "telem", "disk", "tnc", "bcreg"};

std::string execute(std::string cmd, std::string shell);
void send_live(const std::string type, std::string &node_type, std::string &line);
std::string escape_json(const std::string &s);
std::string get_directory(const std::string path);
map<std::string, std::string> get_keys(const std::string &request, const std::string variable_delimiter, const std::string value_delimiter);
void str_to_lowercase(std::string &input);
MongoFindOption option_table(std::string input);
void set_mongo_options(mongocxx::options::find &options, std::string request);

void process_commands(mongocxx::client &connection_file, std::string &realm, std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &file_walk_path, std::string agent_type);

/*! Escape a JSON string
 * \brief escape_json
 * \param s string to escape
 * \return escaped JSON string
 */
std::string escape_json(const std::string &s)
{
    std::ostringstream o;

    for (auto c = s.cbegin(); c != s.cend(); c++) {
        switch (*c) {
            case '\x00': o << "\\u0000"; break;
            case '\x01': o << "\\u0001"; break;
            case '\x0a': o << "\\n"; break;
            case '\x1f': o << "\\u001f"; break;
            case '\x22': o << "\\\""; break;
            case '\x5c': o << "\\\\"; break;
            default: o << *c;
        }
    }

    return o.str();
}

/*! Given a path, get the last directory
 * \brief get_directory
 * \param path
 * \return
 */
std::string get_directory(std::string path) {
    std::size_t directory = path.find_last_of("/\\");

    return path.substr(directory + 1);
}

/*! Run a command line script and get the output of it.
 * \brief execute Use popen to run a command line script and get the output of the command.
 * \param cmd the command to run
 * \return the output from the command that was run
 */
std::string execute(std::string cmd, std::string shell) {
    try {
        std::string data;
        FILE * stream;
        const int max_buffer = 256;
        char buffer[max_buffer];
        cmd.insert(0, shell + " -c ");
        cmd.append(" 2>&1");

        stream = popen(cmd.c_str(), "r");
        if (stream)
        {
            while (!feof(stream))
            {
                if (fgets(buffer, max_buffer, stream) != NULL) data.append(buffer);
            }
            pclose(stream);
        }

        return data;
    } catch (...) {
        return std::string();
    }
}

/*! Send a message through the websocket.
 * \brief send_live
 * \param type type of log
 * \param node_type type of message it will be sent as
 * \param line message to send
 */
void send_live(const std::string type, std::string &node_type, std::string &line) {
    using namespace std;

    std::string ip = "localhost:8081/live/" + node_type;
    // Websocket client here to broadcast to the WS server, then the WS server broadcasts to all clients that are listening
//    WsClient client(ip);

//    client.on_open = [type, &line, &node_type](std::shared_ptr<WsClient::Connection> connection)
//    {
//        connection->send(line);

//        connection->send_close(1000);
//    };

//    client.on_close = [](std::shared_ptr<WsClient::Connection> /*connection*/, int status, const std::string & /*reason*/)
//    {
//        if (status != 1000) {
//            cout << "WS Live: Closed connection with status code " << status << endl;
//        }
//    };

//    // See http://www.boost.org/doc/libs/1_55_0/doc/html/boost_asio/reference.html, Error Codes for error code meanings
//    client.on_error = [](std::shared_ptr<WsClient::Connection> /*connection*/, const SimpleWeb::error_code &ec)
//    {
//        cout << "WS Live: Error: " << ec << ", error message: " << ec.message() << endl;
//    };

//    client.start();
}

/*! Check whether a vector contains a certain value.
 * \brief vector_contains loop through the vector and check that a certain value is identical to one inside the vector
 * \param input_vector
 * \param value
 * \return boolean: true if it was found inthe vector, false if not.
 */
bool vector_contains(std::vector<std::string> &input_vector, std::string value)
{
    for (std::vector<std::string>::iterator it = input_vector.begin(); it != input_vector.end(); ++it)
    {
        if (*it == value)
        {
            return true;
        }
    }
    return false;
}


/*! Check whether to save data from a node from command line/file specification
 * \brief whitelisted_node loop through the included/excluded vectors and check if the node is contained in either one.
 * \param included_nodes vector of included nodes
 * \param excluded_nodes vector of excluded nodes
 * \param node the node to check against vectors
 * \return whether the node is whitelisted; true if it is, false if it is not.
 */
bool whitelisted_node(std::vector<std::string> &included_nodes, std::vector<std::string> &excluded_nodes, std::string &node) {
    bool whitelisted = false;

    // Check if the node is on the included list, if so return true

    // if not, continue and check if included list contains the wildcard

    // if it contains the wildcard, check if the node is on the excluded list

    if (vector_contains(included_nodes, node)) {
        whitelisted = true;
    } else {
        if (vector_contains(excluded_nodes, node)) {
            whitelisted = false;
        } else if (vector_contains(included_nodes, "*")) {
            whitelisted = true;
        }
    }

    return whitelisted;
}

//! Retrieve a request consisting of a list of key values and assign them to a map.
 /*! \brief Split up the list of key values by a specified delimiter, then split up the key values by a specified delimiter and assign the key to a map with its corresponding value.
 \param request The request to assign to a map
 \param variable_delimiter The delimiter that separates the list of key values
 \param value_delimiter The delimiter that separates the key from the value
 \return map<std::string, std::string>
*/

std::map<std::string, std::string> get_keys(const std::string &request, const std::string variable_delimiter, const std::string value_delimiter) {
    using namespace std;

    // Delimit by key value pairs
    vector<std::string> input = string_split(request, variable_delimiter);
    map<std::string, std::string> keys;

    // Delimit
    for (vector<std::string>::iterator it = input.begin(); it != input.end(); ++it) {
        vector<std::string> kv = string_split(*it, value_delimiter);

        // Set the variable to the map key and assign the corresponding value
        keys[kv[0]] = kv[1];
    }

    return keys;
}

//! Convert the characters in a given string to lowercase
/*!
 \param input The string to convert to lowercase
 \return void
*/
void str_to_lowercase(std::string &input) {
    std::locale locale;

    for (std::string::size_type i = 0; i < input.length(); ++i) {
        tolower(input[i], locale);
    }
}

//! Convert a given option and return the enumerated value
/*!
   \param input The option
   \return The enumerated MongoDB find option
*/
MongoFindOption option_table(std::string input) {
    str_to_lowercase(input);

    if (input == "allow_partial_results") return MongoFindOption::ALLOW_PARTIAL_RESULTS;
    if (input == "batch_size") return MongoFindOption::BATCH_SIZE;
    if (input == "coalition") return MongoFindOption::COLLATION;
    if (input == "comment") return MongoFindOption::COMMENT;
    if (input == "cursor_type") return MongoFindOption::CURSOR_TYPE;
    if (input == "hint") return MongoFindOption::HINT;
    if (input == "limit") return MongoFindOption::LIMIT;
    if (input == "max") return MongoFindOption::MAX;
    if (input == "max_await_time") return MongoFindOption::MAX_AWAIT_TIME;
    if (input == "max_scan") return MongoFindOption::MAX_SCAN;
    if (input == "max_time") return MongoFindOption::MAX_TIME;
    if (input == "min") return MongoFindOption::MIN;
    if (input == "no_cursor_timeout") return MongoFindOption::NO_CURSOR_TIMEOUT;
    if (input == "projection") return MongoFindOption::PROJECTION;
    if (input == "read_preferences") return MongoFindOption::READ_PREFERENCE;
    if (input == "modifiers") return MongoFindOption::MODIFIERS;
    if (input == "return_key") return MongoFindOption::RETURN_KEY;
    if (input == "show_record_id") return MongoFindOption::SHOW_RECORD_ID;
    if (input == "skip") return MongoFindOption::SKIP;
    if (input == "snapshot") return MongoFindOption::SNAPSHOT;
    if (input == "sort") return MongoFindOption::SORT;

    return MongoFindOption::INVALID;
}

//! Set the MongoDB find options in the option class given a JSON object of options
/*!
  \param options The MongoDB find option class to append the options to
  \param request A JSON object of wanted options
*/
void set_mongo_options(mongocxx::options::find &options, std::string request) {
    try {
        bsoncxx::document::value json = bsoncxx::from_json(request);
        bsoncxx::document::view opt { json.view() };

        for (auto e : opt) {
            std::string key = std::string(e.key());

            MongoFindOption option = option_table(key);

            switch(option) {
                case MongoFindOption::ALLOW_PARTIAL_RESULTS:
                if (e.type() == type::k_int32) {
                    options.allow_partial_results(e.get_int32().value);
                } else if (e.type() == type::k_int64) {
                    options.allow_partial_results(e.get_int64().value);
                }
                break;
                case MongoFindOption::BATCH_SIZE:
                if (e.type() == type::k_bool) {
                    options.batch_size(e.get_bool().value);
                }
                break;
            //            case MongoFindOption::COLLATION:
            //                options.batch_size(e.get_int32().value); // string view or value
            //                break;
                case MongoFindOption::LIMIT:
                if (e.type() == type::k_int32) {
                    options.limit(e.get_int32().value);
                } else if (e.type() == type::k_int64) {
                    options.limit(e.get_int64().value);
                }
                break;
                case MongoFindOption::MAX:
                if (e.type() == type::k_document) {
                    options.max(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
                }
                break;
            //            case MongoFindOption::MAX_AWAIT_TIME:
            //                options.max_await_time(e.get_date()); // chronos
            //            case MongoFindOption::MAX_TIME:server
            //                options.max_time() // chronos
                case MongoFindOption::MIN:
                if (e.type() == type::k_document) {
                    options.min(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
                }
                break;
                case MongoFindOption::NO_CURSOR_TIMEOUT:
                if (e.type() == type::k_bool) {
                    options.no_cursor_timeout(e.get_bool().value);
                }
                break;
                case MongoFindOption::PROJECTION:
                // need to convert document to string then back to document view
                if (e.type() == type::k_document) {
                    options.projection(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
                }
                break;
                case MongoFindOption::RETURN_KEY:
                if (e.type() == type::k_bool) {
                    options.return_key(e.get_bool().value);
                }
                break;
                case MongoFindOption::SHOW_RECORD_ID:
                if (e.type() == type::k_bool) {
                    options.show_record_id(e.get_bool().value);
                }
                break;
                case MongoFindOption::SKIP:
                if (e.type() == type::k_int32) {
                    options.skip(e.get_int32().value);
                } else if (e.type() == type::k_int64) {
                    options.skip(e.get_int64().value);
                }
                break;
                case MongoFindOption::SORT:
                if (e.type() == type::k_document) {
                    options.sort(bsoncxx::from_json(bsoncxx::to_json(e.get_document()))); // bson view or value
                }
                break;
                default:
                break;
            }
        }
    } catch (bsoncxx::exception err) {
        cout << err.what() << " - Error parsing MongoDB find options" << endl;
    }
}

#endif // AGENT_MONGO_H
