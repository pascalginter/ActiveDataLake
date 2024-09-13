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

#include "avro/ValidSchema.hh"
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>

#include "avro_headers/manifest.hpp"

#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "../../dto/BlobDto.hpp"
#include "../../dto/RequestDto.hpp"
#include "oatpp/json/Serializer.hpp"

#include "../../virtualization/VirtualizedFile.hpp"
#include "../S3InterfaceUtils.hpp"
#include "../../dto/IcebergMetadataDto.hpp"

class IcebergCatalogController : public oatpp::web::server::api::ApiController {
public:
    explicit IcebergCatalogController(const std::shared_ptr<oatpp::web::mime::ContentMappers>& apiContentMappers)
            : oatpp::web::server::api::ApiController(apiContentMappers)
    {}

    std::string buffer;
private:
    oatpp::json::ObjectMapper objectMapper;
public:

    static std::shared_ptr<IcebergCatalogController> createShared(
            OATPP_COMPONENT(std::shared_ptr<oatpp::web::mime::ContentMappers>, apiContentMappers) // Inject ContentMappers
    ){
        return std::make_shared<IcebergCatalogController>(apiContentMappers);
    }

    ENDPOINT("HEAD", "/metadata/{fileName}", headMetadata,
             PATH(String, fileName)){
        std::cout << "metadata head request " << fileName->c_str() << std::endl;
        if (fileName->ends_with(".json")){
            IcebergMetadataDto::Wrapper metadata = IcebergMetadataDto::createShared();
            metadata->schemas->push_back(IcebergSchemaDto::createShared());
            metadata->snapshots->push_back(IcebergSnapshotDto::createShared());
            buffer = objectMapper.writeToString(metadata);
        }else if (fileName->ends_with(".avro")){
            avro::ValidSchema schema;
            std::ifstream in("../avro_schemas/manifest-list.json");

            avro::compileJsonSchema(in, schema);
            manifest_file manifest;
            std::unique_ptr<avro::OutputStream> out = avro::fileOutputStream("temp.avro");
            avro::DataFileWriter<manifest_file> dataFileWriter(std::move(out), schema);

            dataFileWriter.write(manifest);
            dataFileWriter.flush();
            dataFileWriter.close();

            std::ifstream avroIn("temp.avro");
            buffer = std::string((std::istreambuf_iterator<char>(avroIn)), std::istreambuf_iterator<char>());
        }

        auto response = createResponse(Status::CODE_200, "");
        std::cout << buffer.size() << std::endl;
        S3InterfaceUtils::putByteSizeHeader(response, buffer.size());
        return response;
    }

    ENDPOINT("GET", "/metadata/{fileName}", getMetadata,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        auto range = S3InterfaceUtils::extractRange(request);
        std::cout << range.begin << " " << range.end << " (of " << buffer.size() << ")" << std::endl;
        return createResponse(Status::CODE_200, buffer);
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //ICEBERG_CATALOG_CONTROLLER_HPP