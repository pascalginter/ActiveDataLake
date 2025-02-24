#include <oatpp/web/server/api/ApiController.hpp>

#ifndef S3InterfaceUtils_H
#define S3InterfaceUtils_H

class S3InterfaceUtils {
public:
    struct ByteRange {
        int64_t begin;
        int64_t end;

        [[nodiscard]] int64_t size() const { return end - begin + 1; };
        [[nodiscard]] std::string toRangeString() const { return "bytes=" + std::to_string(begin) + "-" + std::to_string(end);}
    };

    static ByteRange extractRange(
            const std::shared_ptr<oatpp::web::server::api::ApiController::IncomingRequest>& request,
            int64_t defaultEndDelimiter){
        if (request->getHeaders().getAll().contains("range")){
            std::string range = request->getHeader("range");
            size_t firstSep = range.find('=');
            size_t secondSep = range.find('-');
            int64_t rangeBegin = std::stoll(range.substr(firstSep + 1, secondSep - firstSep));
            int64_t rangeEnd = std::stoll(range.substr(secondSep+1));
            return {rangeBegin, rangeEnd};
        }else{
            return {0, defaultEndDelimiter ? defaultEndDelimiter - 1 : 0};
        }

    }

    static void putByteSizeHeader(std::shared_ptr<oatpp::web::server::api::ApiController::OutgoingResponse>& response,
                            size_t contentSize){
        response->putHeader("Content-Length", std::to_string(contentSize));
    }
};

#endif