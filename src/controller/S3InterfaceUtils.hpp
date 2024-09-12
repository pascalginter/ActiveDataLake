#include <oatpp/web/server/api/ApiController.hpp>

#ifndef S3InterfaceUtils_H
#define S3InterfaceUtils_H

class S3InterfaceUtils {
public:
    struct ByteRange {
        size_t begin;
        size_t end;

        [[nodiscard]] size_t size() const { return end - begin + 1; };
    };

    static ByteRange extractRange(
            const std::shared_ptr<oatpp::web::server::api::ApiController::IncomingRequest>& request){
        std::string range = request->getHeaders().get("range");
        size_t firstSep = range.find('=');
        size_t secondSep = range.find('-');
        size_t rangeBegin = std::stoll(range.substr(firstSep + 1, secondSep - firstSep));
        size_t rangeEnd = std::stoll(range.substr(secondSep+1));
        return {rangeBegin, rangeEnd};
    }

    static void putByteSizeHeader(std::shared_ptr<oatpp::web::server::api::ApiController::OutgoingResponse>& response,
                            size_t contentSize){
        response->putHeader("Content-Length", std::to_string(contentSize));
    }
};

#endif