#ifndef ICEBERG_CATALOG_CONTROLLER_HPP
#define ICEBERG_CATALOG_CONTROLLER_HPP


#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "../../dto/IcebergRestDtos/CatalogConfig.hpp"
#include "../../dto/IcebergRestDtos/CommitTableRequest.hpp"
#include "../../dto/IcebergRestDtos/CreateNamespaceRequest.hpp"
#include "../../dto/IcebergRestDtos/CreateTableRequest.hpp"
#include "../../dto/IcebergRestDtos/CreateTableResponse.hpp"
#include "../../dto/IcebergRestDtos/ErrorResponse.hpp"
#include "../../dto/IcebergRestDtos/ListNamespacesResponse.hpp"
#include "../../dto/IcebergRestDtos/GetNamespaceResponseDto.hpp"
#include "../../dto/IcebergRestDtos/ListTablesResponse.hpp"
#include "../../dto/IcebergRestDtos/LoadTableResponse.hpp"

#include "../../util/uuid.hpp"

class IcebergCatalogController final : public oatpp::web::server::api::ApiController {
    static thread_local pqxx::connection conn;
    static thread_local String buffer;
    std::atomic<int64_t> ref = 0;

public:
    explicit IcebergCatalogController(const std::shared_ptr<oatpp::web::mime::ContentMappers>& apiContentMappers)
            : ApiController(apiContentMappers)
    {}

private:
    oatpp::json::ObjectMapper objectMapper;
public:
    static std::shared_ptr<IcebergCatalogController> createShared(
            OATPP_COMPONENT(std::shared_ptr<oatpp::web::mime::ContentMappers>, apiContentMappers) // Inject ContentMappers
    ){
        return std::make_shared<IcebergCatalogController>(apiContentMappers);
    }

    ENDPOINT("GET", "/v1/config", getConfig){
        std::cout << "Get config" << std::endl;
        Config config;
        const nlohmann::json configJson = config;
        buffer = configJson.dump();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("GET", "/v1/namespaces", listNamespaces){
        std::cout << "List namespaces" << std::endl;
        ListNamespacesResponse response;
        pqxx::work tx{conn};
        for (const auto& [name] : tx.query<std::string>("SELECT name FROM namespace")) {
            response.namespaces.push_back({name});
        }
        tx.commit();
        const nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        return createDtoResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("POST", "/v1/namespaces", postNamespaces,
            REQUEST(std::shared_ptr<IncomingRequest>, request)) {
        std::cout << "post namespace" << std::endl;
        const CreateNamespaceRequest cnq = nlohmann::json::parse(*request->readBodyToString());
        assert(cnq.namespaces.size() == 1);
        pqxx::work tx{conn};
        tx.exec("INSERT INTO namespaces(name) VALUES ($1)", pqxx::params{cnq.namespaces.front()});
        tx.commit();
        return createResponse(Status::CODE_200);
    }

    ENDPOINT("POST", "v1/namespaces/{nspace}/tables", postTable,
            PATH(String, nspace),
            REQUEST(std::shared_ptr<IncomingRequest>, request)) {
        const CreateTableRequest tableRequest = nlohmann::json::parse(*request->readBodyToString());
        std::cout << "post table " << nspace->c_str() << " " << tableRequest.name << std::endl;
        IcebergMetadata metadata;
        metadata.schemas.push_back(tableRequest.schema);
        assert(tableRequest.partitionSpec && tableRequest.writeOrder);
        metadata.partitionSpecs.push_back(*tableRequest.partitionSpec);
        metadata.sortOrders.push_back(*tableRequest.writeOrder);
        std::string serializedMetadata = nlohmann::json(metadata);

        pqxx::work tx{conn};
        tx.exec("INSERT INTO tables(table_uuid, name, last_sequence_number, last_updated_ms, current_snapshot_id) VALUES ($1, $2, $3, $4, $5)",
               pqxx::params{uuid::generate_uuid_v4(), tableRequest.name, 0, 0, 0});

        ref = metadata.currentSnapshotId;
        UpdateTableResponse response;
        response.metadata = metadata;
        response.metadataLocation = "./metadata/v1.json";
        nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        tx.commit();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("POST", "v1/namespaces/{nspace}/tables/{table}", commit,
            PATH(String, nspace), PATH(String, table),
            REQUEST(std::shared_ptr<IncomingRequest>, request)) {
        std::cout << "Commit table version " << table->c_str() << std::endl;
        static std::atomic<int32_t> fileId = 0;
        CommitTableRequest commitRequest = nlohmann::json::parse(*request->readBodyToString());
        assert(commitRequest.requirements.size() == 2);
        assert(commitRequest.requirements[0].type.starts_with("assert-ref-snapshot-id"));
        assert(commitRequest.requirements[1].type.starts_with("assert-table-uuid"));

        if (ref != commitRequest.requirements[0].snapshotId) {
            ErrorResponse response("The commit has failed", "CommitFailedException", 409);
            nlohmann::json responseJson = response;
            buffer = responseJson.dump();
            return createResponse(Status::CODE_409, buffer);
        }
        pqxx::work tx{conn};
        IcebergMetadata metadata = nlohmann::json(std::get<0>(
            tx.query1<std::string>("SELECT metadata FROM tables WHERE name = $1", pqxx::params{table->c_str()})));
        metadata.snapshots.push_back(commitRequest.updates[0].snapshot);
        metadata.currentSnapshotId = metadata.snapshots.size();

        ++fileId;
        std::string location = "./metadata/" + std::to_string(fileId) + ".json";
        UpdateTableResponse response;
        response.metadata = metadata;
        response.metadataLocation = location;
        nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        std::ofstream out(location);
        out << buffer->c_str();

        if (ref.compare_exchange_weak(commitRequest.requirements[0].snapshotId, metadata.currentSnapshotId)) {
            std::string serializedMetadata = nlohmann::json(metadata);
            tx.exec("UPDATE tables SET metadata = $1 WHERE name = $2", pqxx::params(serializedMetadata, table->c_str()));
            tx.commit();
            ref = metadata.currentSnapshotId;
            return createResponse(Status::CODE_200, buffer);
        } else {
            ErrorResponse errorResponse("The commit has failed", "CommitFailedException", 409);
            nlohmann::json errorResponseJson = errorResponse;
            buffer = errorResponseJson.dump();
            return createResponse(Status::CODE_409, buffer);
        }
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}", getNamespace,
             PATH(String, nspace)){
        std::cout << "Get namespace " << nspace->c_str() << std::endl;
        return createDtoResponse(Status::CODE_200, GetNamespaceResponseDto::createShared());
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/tables", listTables,
             PATH(String, nspace)){
        std::cout << "List tables " << nspace->c_str() << std::endl;
        ListTablesResponse response;
        pqxx::work tx{conn};
        for (const auto& [name] : tx.query<std::string>("SELECT name FROM tables")) {
            response.identifiers.emplace_back();
            response.identifiers.back().name = name;
            response.identifiers.back().nspace = {nspace};
        }
        tx.commit();
        const nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/tables/{table}", getTable,
             PATH(String, nspace), PATH(String, table)){
        std::cout << "Load table " << nspace->c_str() << " " << table->c_str() << std::endl;
        LoadTableResponse response;
        response.metadataLocation = "./metadata/v1.json";
        pqxx::work tx{conn};
        response.metadata = nlohmann::json(std::get<0>(
            tx.query1<std::string>("SELECT metadata FROM tables WHERE name = $1", pqxx::params{table->c_str()})));
        const nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        tx.commit();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/views", listViews,
             PATH(String, nspace)){
        std::cout << "List views " << nspace->c_str() << std::endl;
        ListTablesResponse response;
        const nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/views/{view}", loadView){
        std::cout << "Load view" << std::endl;
        return createResponse(Status::CODE_404);
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //ICEBERG_CATALOG_CONTROLLER_HPP