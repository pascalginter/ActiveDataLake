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

class IcebergCatalogController final : public oatpp::web::server::api::ApiController {
    static thread_local pqxx::connection conn;
    static std::unordered_map<String, std::vector<IcebergMetadata>> namespaces;
    static thread_local String buffer;
    std::atomic<int64_t> ref = 0;
    std::atomic<int64_t> successfulCommits = 0;
    std::atomic<int64_t> failedCommits = 0;
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

    ENDPOINT("GET", "/v1/config", getConfig){
        std::cout << "Get config" << std::endl;
        Config config;
        const nlohmann::json configJson = config;
        buffer = configJson.dump();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("GET", "/v1/namespaces", listNamespaces,
             QUERY(String, pageToken)){
        std::cout << "List namespaces, page token = " << pageToken->c_str() << std::endl;
        ListNamespacesResponse response;
        const nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        return createDtoResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("POST", "/v1/namespaces", postNamespaces,
            REQUEST(std::shared_ptr<IncomingRequest>, request)) {
        std::cout << "post namespace" << std::endl;
        const CreateNamespaceRequest cnq = nlohmann::json::parse(*request->readBodyToString());
        assert(cnq.namespaces.size() == 1);
        namespaces[cnq.namespaces[0]] = {};
        return createResponse(Status::CODE_200);
    }

    ENDPOINT("POST", "v1/namespaces/{nspace}/{table}", postTable,
            PATH(String, nspace), PATH(String, table),
            REQUEST(std::shared_ptr<IncomingRequest>, request)) {
        std::cout << "post table" << std::endl;
        const CreateTableRequest tableRequest = nlohmann::json::parse(*request->readBodyToString());
        IcebergMetadata metadata;
        metadata.schemas.push_back(tableRequest.schema);
        assert(tableRequest.partitionSpec && tableRequest.writeOrder);
        metadata.partitionSpecs.push_back(*tableRequest.partitionSpec);
        metadata.sortOrders.push_back(*tableRequest.writeOrder);
        namespaces[nspace].push_back(metadata);

        ref = metadata.currentSnapshotId;
        UpdateTableResponse response;
        response.metadata = metadata;
        response.metadataLocation = "./metadata/v1.json";
        nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        return createResponse(Status::CODE_200, buffer);
    }

    ENDPOINT("POST", "v1/namespaces/{nspace}/tables/{table}", commit,
            PATH(String, nspace), PATH(String, table),
            REQUEST(std::shared_ptr<IncomingRequest>, request)) {
        CommitTableRequest commitRequest = nlohmann::json::parse(*request->readBodyToString());
        assert(commitRequest.requirements.size() == 2);
        assert(commitRequest.requirements[0].type.starts_with("assert-ref-snapshot-id"));
        assert(commitRequest.requirements[1].type.starts_with("assert-table-uuid"));

        if (ref != commitRequest.requirements[0].snapshotId) {
            ++failedCommits;
            ErrorResponse response("The commit has failed", "CommitFailedException", 409);
            nlohmann::json responseJson = response;
            buffer = responseJson.dump();
            return createResponse(Status::CODE_409, buffer);
        }

        auto& metadata = namespaces[nspace][0];
        metadata.snapshots.push_back(commitRequest.updates[0].snapshot);
        metadata.currentSnapshotId = metadata.snapshots.size();

        UpdateTableResponse response;
        response.metadata = metadata;
        response.metadataLocation = "./metadata/v1.json";
        nlohmann::json responseJson = response;
        buffer = responseJson.dump();
        std::ofstream out("./metadata/v1.json");
        out << buffer->c_str();

        if (ref.compare_exchange_weak(commitRequest.requirements[0].snapshotId, metadata.currentSnapshotId)) {
            ++successfulCommits;
            ref = metadata.currentSnapshotId;
            return createResponse(Status::CODE_200, buffer);
        } else {
            ++failedCommits;
            ErrorResponse errorResponse("The commit has failed", "CommitFailedException", 409);
            nlohmann::json errorResponseJson = errorResponse;
            buffer = errorResponseJson.dump();
            return createResponse(Status::CODE_409, buffer);
        }
    }

    ENDPOINT("GET", "/v1/statistics", getStatistics) {
        nlohmann::json json = {
            {"success", successfulCommits.load()},
            {"failed", failedCommits.load()}
        };
        buffer = json.dump();
        return createResponse(Status::CODE_200, buffer);
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
        for (const auto& [name] : tx.query<std::string>("SELECT name FROM table")) {
            response.identifiers.emplace_back();
            response.identifiers.back().name = name;
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
        response.metadata = namespaces[nspace->c_str()][0];
        const nlohmann::json responseJson = response;
        buffer = responseJson.dump();
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