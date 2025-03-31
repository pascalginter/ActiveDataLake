#ifndef ICEBERG_METADATA_CONTROLLER_HPP
#define ICEBERG_METADATA_CONTROLLER_HPP

#include <fstream>
#include <sys/types.h>

#include <iostream>

#include <btrblocks/arrow/DirectoryReader.hpp>

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/json/ObjectMapper.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/macro/component.hpp"

#include <avro/ValidSchema.hh>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>

#include "avro_headers/manifest_file.hpp"
#include "avro_headers/manifest_list.hpp"

#include <pqxx/pqxx>

#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "oatpp/json/Serializer.hpp"

#include "../../dto/IcebergMetadataDto.hpp"

class IcebergMetadataController final : public oatpp::web::server::api::ApiController {
    static thread_local pqxx::connection conn;
    ;
public:
    explicit IcebergMetadataController(const std::shared_ptr<oatpp::web::mime::ContentMappers>& apiContentMappers)
            : oatpp::web::server::api::ApiController(apiContentMappers)
    {}

    std::string buffer;

    void prepareMetadata(const std::string& tableName){
        pqxx::work tx(conn);
        const pqxx::row table = tx.exec(""
                                  "SELECT table_uuid, last_sequence_number, last_updated_ms, current_snapshot_id"
                                  "FROM table"
                                  "WHERE name = $1", pqxx::params{tableName}
        ).one_row();
        const IcebergMetadataDto::Wrapper metadata = IcebergMetadataDto::createShared();
        metadata->tableUUID = table[0].as<std::string>();
        metadata->lastSequenceNumber = table[1].as<int>();
        metadata->lastUpdatedMs = table[2].as<int>();
        metadata->currentSnapshotId = table[3].as<int>();

        for (const auto& [snapshotId, sequenceNumber, timestampMs, manifestList, summaryOperation] :
            tx.query<int, int, int, std::string, std::string>(""
                "SELECT snapshot_id, sequence_number, timestamp_ms, manifest_list, summary_operation "
                "FROM table"
                "WHERE table_name = $1", pqxx::params{tableName})) {
            auto dto = IcebergSnapshotDto::createShared();
            dto->snapshotId = snapshotId;
            dto->sequenceNumber = sequenceNumber;
            dto->timestampMs = timestampMs;
            dto->summary->operation = summaryOperation;
            metadata->snapshots->push_back(dto);
        }
        tx.commit();
        buffer = objectMapper.writeToString(metadata);
    }

    void prepareManifestList(){
        avro::ValidSchema schema;
        std::ifstream in("../avro_schemas/manifest-list.json");

        avro::compileJsonSchema(in, schema);
        manifest_file manifest;
        manifest.manifest_path = "http://localhost:8000/manifest-file/file.avro";
        manifest.manifest_length = 1000;
        manifest.partition_spec_id = 0;
        manifest.content = 0;
        manifest.sequence_number = 0;
        manifest.min_sequence_number = 0;
        manifest.added_snapshot_id = 0;
        manifest.existing_data_files_count = 1;
        manifest.deleted_data_files_count = 0;
        manifest.added_rows_count = 0;
        manifest.existing_rows_count = 600000;
        manifest.deleted_rows_count = 0;
        r508 summary;
        summary.contains_null = false;
        summary.contains_nan.set_bool(false);
        summary.lower_bound.set_null();
        summary.upper_bound.set_null();
        manifest.partitions.set_array({summary});
        std::unique_ptr<avro::OutputStream> out = avro::fileOutputStream("temp.avro");
        avro::DataFileWriter<manifest_file> dataFileWriter(std::move(out), schema);

        dataFileWriter.write(manifest);
        dataFileWriter.flush();
        dataFileWriter.close();

        std::ifstream avroIn("temp.avro");
        buffer = std::string((std::istreambuf_iterator<char>(avroIn)), std::istreambuf_iterator<char>());
    }

    void prepareManifestFile(){
        std::cout << "head manifest-file" << std::endl;
        avro::ValidSchema schema;
        std::ifstream in("../avro_schemas/manifest-file.json");
        avro::compileJsonSchema(in, schema);

        manifest_entry manifest;
        manifest.data_file.file_path = "http://localhost:8000/data/lineitem.parquet";
        std::unique_ptr<avro::OutputStream> out = avro::fileOutputStream("temp.avro");
        avro::DataFileWriter<manifest_entry> dataFileWriter(std::move(out), schema);

        dataFileWriter.write(manifest);
        dataFileWriter.flush();
        dataFileWriter.close();

        std::ifstream avroIn("temp.avro");
        buffer = std::string((std::istreambuf_iterator<char>(avroIn)), std::istreambuf_iterator<char>());
    }

    [[nodiscard]] std::shared_ptr<OutgoingResponse> respondWithBufferSize(){
        auto response = createResponse(Status::CODE_200, "");
        std::cout << buffer.size() << std::endl;
        S3InterfaceUtils::putByteSizeHeader(response, buffer.size());
        return response;
    }
private:
    oatpp::json::ObjectMapper objectMapper;
public:

    static std::shared_ptr<IcebergMetadataController> createShared(
            OATPP_COMPONENT(std::shared_ptr<oatpp::web::mime::ContentMappers>, apiContentMappers) // Inject ContentMappers
    ){
        return std::make_shared<IcebergMetadataController>(apiContentMappers);
    }

    ENDPOINT("HEAD", "/metadata/{fileName}", headMetadata,
             PATH(String, fileName)) {
        assert(fileName->ends_with(".json"));
        std::cout << "head metadata" << std::endl;
        prepareMetadata(fileName);
        return respondWithBufferSize();
    }

    ENDPOINT("HEAD", "/manifest-list/{fileName}", headManifestList,
             PATH(String, fileName)) {
        assert(fileName->ends_with(".avro"));
        std::cout << "head manifest-list" << std::endl;
        prepareManifestList();
        return respondWithBufferSize();
    }

    ENDPOINT("HEAD", "/manifest-file/{fileName}", headManifestFile,
             PATH(String, fileName)){
        assert(fileName->ends_with(".avro"));
        std::cout << "head manifest-file" << std::endl;
        prepareManifestFile();
        return respondWithBufferSize();
    }

    ENDPOINT("GET", "/metadata/{fileName}", getMetadata,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        std::cout << "get metadata" << std::endl;
        prepareMetadata(fileName);
        const auto range = S3InterfaceUtils::extractRange(request, buffer.size());
        std::cout << range.begin << " " << range.end << " (of " << buffer.size() << ")" << std::endl;
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("GET", "/manifest-list/{fileName}", getManifestList,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        std::cout << "get manifest-list " << fileName->c_str() << std::endl;
        prepareManifestList();
        auto range = S3InterfaceUtils::extractRange(request, buffer.size());
        std::cout << range.begin << " " << range.end << " (of " << buffer.size() << ")" << std::endl;
        auto response = createResponse(Status::CODE_200, buffer);
        S3InterfaceUtils::putByteSizeHeader(response, buffer.size());
        return response;
    }

    ENDPOINT("GET", "/manifest-file/{fileName}", getManifestFile,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        std::cout << "get manifest-file" << std::endl;
        prepareManifestFile();
        auto range = S3InterfaceUtils::extractRange(request, buffer.size());
        std::cout << range.begin << " " << range.end << " (of " << buffer.size() << ")" << std::endl;
        return createResponse(Status::CODE_200, buffer);
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //ICEBERG_METADATA_CONTROLLER_HPP