#ifndef ICEBERG_CATALOG_CONTROLLER_HPP
#define ICEBERG_CATALOG_CONTROLLER_HPP

#include <fstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <btrblocks/arrow/DirectoryReader.hpp>
#include <btrblocks.hpp>

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/json/ObjectMapper.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/macro/component.hpp"

#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "../../dto/BlobDto.hpp"
#include "../../dto/RequestDto.hpp"
#include "oatpp/json/Serializer.hpp"

#include "../../virtualization/VirtualizedFile.hpp"
#include "../S3InterfaceUtils.hpp"

std::shared_ptr<arrow::io::BufferReader> buffer_reader = nullptr;

void prepareInMemoryParquet(){
    btrblocks::arrow::DirectoryReader directory("../data/lineitem");
    std::shared_ptr<arrow::Table> table;
    directory.ReadTable(&table);
    std::cout << table->schema()->ToString() << std::endl;

    std::shared_ptr<arrow::io::BufferOutputStream> buffer_output_stream;
    PARQUET_ASSIGN_OR_THROW(buffer_output_stream, arrow::io::BufferOutputStream::Create());

    // Write Arrow table to in-memory parquet
    parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), buffer_output_stream);



    // Get the buffer containing parquet data
    std::shared_ptr<arrow::Buffer> buffer;
    PARQUET_ASSIGN_OR_THROW(buffer, buffer_output_stream->Finish());
    buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
}

class IcebergCatalogController : public oatpp::web::server::api::ApiController {
public:
    explicit IcebergCatalogController(const std::shared_ptr<oatpp::web::mime::ContentMappers>& apiContentMappers)
            : oatpp::web::server::api::ApiController(apiContentMappers)
    {}
private:
    oatpp::json::ObjectMapper objectMapper;
    std::unordered_map<std::string, std::shared_ptr<VirtualizedFile>> tableFiles;

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

    ENDPOINT("HEAD", "/data/{fileName}", headData,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        std::string_view fileNameView(fileName->c_str());
        if (!fileNameView.ends_with(".parquet")){
            return createResponse(Status::CODE_501, "File format is not supported");
        }
        std::string tableName(fileNameView.begin(), fileName->size() - 8);
        if (tableFiles.find(tableName) == tableFiles.end()){
            tableFiles[tableName] = VirtualizedFile::createFileAbstraction(tableName);
        }
        auto& file = tableFiles[tableName];
        auto response = createResponse(Status::CODE_200, "");
        S3InterfaceUtils::putByteSizeHeader(response, file->size());
        return response;
    }

    ENDPOINT("GET", "/data/{fileName}", getData,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        std::string_view fileNameView(fileName->c_str());
        if (!fileNameView.ends_with(".parquet")){
            return createResponse(Status::CODE_501, "File format is not supported");
        }
        std::string tableName(fileNameView.begin(), fileName->size() - 8);
        if (tableFiles.find(tableName) == tableFiles.end()){
            std::cout << "create abstraction" << std::endl;
            tableFiles[tableName] = VirtualizedFile::createFileAbstraction(tableName);
        }
        auto range = S3InterfaceUtils::extractRange(request);
        auto response = createResponse(Status::CODE_200, tableFiles[tableName]->getRange(range));
        S3InterfaceUtils::putByteSizeHeader(response, tableFiles[tableName]->size());
        return response;
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //ICEBERG_CATALOG_CONTROLLER_HPP