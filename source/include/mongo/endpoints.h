#ifndef ENDPOINTS_H
#define ENDPOINTS_H

#include "mongo/agent_mongo.h"

class HttpHandler: public Http::Handler {

    HTTP_PROTOTYPE(HttpHandler)

    void onRequest(
            const Http::Request& req,
            Http::ResponseWriter response) override {

        if (req.resource() == "/ping") {
            if (req.method() == Http::Method::Get) {

                using namespace Http;

                auto query = req.query();
                if (query.has("chunked")) {
                    std::cout << "Using chunked encoding" << std::endl;

                    response.headers()
                        .add<Header::Server>("pistache/0.1")
                        .add<Header::ContentType>(MIME(Text, Plain));

                    response.cookies()
                        .add(Cookie("lang", "en-US"));

                    auto stream = response.stream(Http::Code::Ok);
                    stream << "PO";
                    stream << "NG";
                    stream << ends;
                }
                else {
                    response.send(Http::Code::Ok, "PONG");
                }

            }
        }
        else if (req.resource() == "/echo") {
            if (req.method() == Http::Method::Post) {
                response.send(Http::Code::Ok, req.body(), MIME(Text, Plain));
            } else {
                response.send(Http::Code::Method_Not_Allowed);
            }
        }
        else if (req.resource() == "/stream_binary") {
            auto stream = response.stream(Http::Code::Ok);
            char binary_data[] = "some \0\r\n data\n";
            size_t chunk_size = 14;
            for (size_t i = 0; i < 10; ++i) {
                stream.write(binary_data, chunk_size);
                stream.flush();
            }
            stream.ends();
        }
        else if (req.resource() == "/exception") {
            throw std::runtime_error("Exception thrown in the handler");
        }
        else if (req.resource() == "/timeout") {
            response.timeoutAfter(std::chrono::seconds(2));
        }
        else if (req.resource() == "/static") {
            if (req.method() == Http::Method::Get) {
                Http::serveFile(response, "README.md").then([](ssize_t bytes) {
                    std::cout << "Sent " << bytes << " bytes" << std::endl;
                }, Async::NoExcept);
            }
        } else {
            response.send(Http::Code::Not_Found);
        }

    }

    void onTimeout(
            const Http::Request& req,
            Http::ResponseWriter response) override {
        UNUSED(req);
        response
            .send(Http::Code::Request_Timeout, "Timeout")
            .then([=](ssize_t) { }, PrintException());
    }

};

#endif // ENDPOINTS_H
