
#ifndef CRUD_ERRORHANDLER_HPP
#define CRUD_ERRORHANDLER_HPP

#include <iostream>
#include "dto/StatusDto.hpp"

#include "oatpp/web/server/handler/ErrorHandler.hpp"
#include "oatpp/web/protocol/http/outgoing/ResponseFactory.hpp"
#include "oatpp/web/mime/ContentMappers.hpp"

class ErrorHandler : public oatpp::web::server::handler::DefaultErrorHandler {
private:
    typedef oatpp::web::protocol::http::outgoing::Response OutgoingResponse;
    typedef oatpp::web::protocol::http::Status Status;
    typedef oatpp::web::protocol::http::outgoing::ResponseFactory ResponseFactory;
private:
    std::shared_ptr<oatpp::web::mime::ContentMappers> m_mappers;
public:

    ErrorHandler(const std::shared_ptr<oatpp::web::mime::ContentMappers>& mappers) : m_mappers(mappers){};

    std::shared_ptr<OutgoingResponse> renderError(const HttpServerErrorStacktrace& stacktrace) override {
        Status status = stacktrace.status;
        if(status.description == nullptr) {
            status.description = "Unknown";
        }

        oatpp::data::stream::BufferOutputStream ss;

        for(auto& s : stacktrace.stack) {
            ss << s << "\n";
        }

        auto error = StatusDto::createShared();
        error->status = "ERROR";
        error->code = stacktrace.status.code;
        error->message = ss.toString();

        std::vector<oatpp::String> accept;
        if(stacktrace.request) {
            accept = stacktrace.request->getHeaderValues("Accept");
        }

        auto mapper = m_mappers->selectMapper(accept);
        if(!mapper) {
            mapper = m_mappers->getDefaultMapper();
        }

        auto response = ResponseFactory::createResponse(stacktrace.status,error,mapper);

        for(const auto& pair : stacktrace.headers.getAll()) {
            response->putHeader(pair.first.toString(), pair.second.toString());
        }

        std::cout << "------------------------------------------------" << std::endl;
        for (auto& stackElem : stacktrace.stack){
            std::cout << stackElem.operator std::string() << std::endl;
        }
        std::cout << "body: " << stacktrace.request->readBodyToString().operator std::string() << std::endl;
        std::cout << "------------------------------------------------" << std::endl;

        return response;
    }

};

#endif //CRUD_ERRORHANDLER_HPP