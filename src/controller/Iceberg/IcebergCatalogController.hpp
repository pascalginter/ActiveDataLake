#ifndef ICEBERG_CATALOG_CONTROLLER_HPP
#define ICEBERG_CATALOG_CONTROLLER_HPP

#include <fstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "btrblocks.hpp"


#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/json/ObjectMapper.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/macro/component.hpp"

#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "../../dto/BlobDto.hpp"
#include "../../dto/RequestDto.hpp"
#include "oatpp/json/Serializer.hpp"

class MemoryMappedFile{
private:
    int file;
    void* data;
    uintptr_t size;

public:
    explicit MemoryMappedFile(const char* fileName){

        file = open(fileName, O_RDONLY);
        if (file < 0){
            std::cout << "file < 0" << std::endl;
            exit(2);
        }
        size = lseek(file, 0, SEEK_END);
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, file, 0);
        if (data == MAP_FAILED){
            std::cout << "MAP_FAILED" << std::endl;
            exit(3);
        }
    }
    ~MemoryMappedFile(){
        munmap(data, size);
        close(file);
    }

    const char* begin(){
        return static_cast<char*>(data);
    }

    const char* end(){
        return  static_cast<char*>(data) + size;
    }
};

thread_local MemoryMappedFile mappedFile("../data/lineitem.parquet");

class IcebergCatalogController : public oatpp::web::server::api::ApiController {
public:
    explicit IcebergCatalogController(const std::shared_ptr<oatpp::web::mime::ContentMappers>& apiContentMappers)
            : oatpp::web::server::api::ApiController(apiContentMappers)
    {}
private:
    oatpp::json::ObjectMapper objectMapper;

public:

    static std::shared_ptr<IcebergCatalogController> createShared(
            OATPP_COMPONENT(std::shared_ptr<oatpp::web::mime::ContentMappers>, apiContentMappers) // Inject ContentMappers
    ){
        return std::make_shared<IcebergCatalogController>(apiContentMappers);
    }

    ENDPOINT("GET", "/metadata/{tableName}.json", getMetadata,
             PATH(String, tableName)){
        return createResponse(Status::CODE_200, "OK");
    }

    ENDPOINT("HEAD", "data/lineitem.parquet", headLineitem){
        // Create a response with a 200 OK status.
        auto response = createResponse(Status::CODE_200, "");

        // Set headers like in the S3 HEAD request example
        response->putHeader("x-amz-id-2", "aBcdEfghijkLmnopQrstuVwxyz1234567890abCDEFGhijKlmnOpQrstUvWXYZ==");
        response->putHeader("x-amz-request-id", "1234ABCDE56789FGHIJ0KLMNOPQRS1234TUV5678");
        response->putHeader("Date", "Mon, 02 Sep 2024 15:03:45 GMT");
        response->putHeader("Last-Modified", "Fri, 30 Aug 2024 12:21:15 GMT");
        response->putHeader("ETag", "\"1234567890abcdef1234567890abcdef\"");
        response->putHeader("Accept-Ranges", "bytes");
        response->putHeader("Content-Type", "application/octet-stream");
        response->putHeader("Content-Length", "2713161713");
        response->putHeader("Server", "AmazonS3");

        // Return the response
        return response;
    }

    ENDPOINT("GET", "/data/lineitem.parquet", getLineitem,
             REQUEST(std::shared_ptr<IncomingRequest>, request)){

        std::string range = request->getHeaders().get("range");
        size_t firstSep = range.find('=');
        size_t secondSep = range.find('-');
        long long rangeBegin = std::stoll(range.substr(firstSep + 1, secondSep - firstSep));
        long long rangeEnd = std::stoll(range.substr(secondSep+1));
        size_t size = rangeEnd - rangeBegin + 1;
        auto blobDto = BlobDto::createShared();

        auto response = createResponse(Status::CODE_200, std::string{mappedFile.begin() + rangeBegin, size});
        response->putHeader("Content-Length", std::to_string(size));
        return response;
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //ICEBERG_CATALOG_CONTROLLER_HPP