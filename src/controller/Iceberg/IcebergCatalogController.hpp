#ifndef ICEBERG_CATALOG_CONTROLLER_HPP
#define ICEBERG_CATALOG_CONTROLLER_HPP


#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "../../dto/IcebergRestDtos/CatalogConfigDto.hpp"
#include "../../dto/IcebergRestDtos/ListNamespacesResponseDto.hpp"
#include "../../dto/IcebergRestDtos/GetNamespaceResponseDto.hpp"
#include "../../dto/IcebergRestDtos/ListTablesResponseDto.hpp"
#include "../../dto/IcebergRestDtos/LoadTableResponseDto.hpp"

class IcebergCatalogController : public oatpp::web::server::api::ApiController {
    static thread_local pqxx::connection conn;
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
        std::cout << objectMapper.writeToString(ConfigDto::createShared())->c_str() << std::endl;
        return createDtoResponse(Status::CODE_200, ConfigDto::createShared());
    }

    ENDPOINT("GET", "/v1/namespaces", listNamespaces,
             QUERY(String, pageToken)){
        std::cout << "List namespaces, page token = " << pageToken->c_str() << std::endl;
        std::cout << objectMapper.writeToString(ListNamespacesResponseDto::createShared())->c_str() << std::endl;
        return createDtoResponse(Status::CODE_200, ListNamespacesResponseDto::createShared());
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}", getNamespace,
             PATH(String, nspace)){
        std::cout << "Get namespace " << nspace->c_str() << std::endl;
        return createDtoResponse(Status::CODE_200, GetNamespaceResponseDto::createShared());
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/tables", listTables,
             PATH(String, nspace)){
        std::cout << "List tables " << nspace->c_str() << std::endl;
        const auto responseDto = ListTablesResponseDto::createShared();
        pqxx::work tx{conn};
        for (const auto& [name] : tx.query<std::string>("SELECT name FROM table")) {
            responseDto->identifiers->push_back(TableIdentifierDto::createShared());
            responseDto->identifiers->back()->name = name;
        }
        tx.commit();
        return createDtoResponse(Status::CODE_200, responseDto);
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/tables/{table}", getTable,
             PATH(String, nspace), PATH(String, table)){
        std::cout << "Load table " << nspace->c_str() << " " << table->c_str() << std::endl;
        std::cout << objectMapper.writeToString(LoadTableResponseDto::createShared())->c_str() << std::endl;
        return createDtoResponse(Status::CODE_200, LoadTableResponseDto::createShared());
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/views", listViews,
             PATH(String, nspace)){
        std::cout << "List views " << nspace->c_str() << std::endl;
        std::cout << objectMapper.writeToString(ListTablesResponseDto::createShared())->c_str() << std::endl;
        return createDtoResponse(Status::CODE_200, ListTablesResponseDto::createShared());
    }

    ENDPOINT("GET", "/v1/namespaces/{nspace}/views/{view}", loadView){
        std::cout << "Load view" << std::endl;
        return createResponse(Status::CODE_404);
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //ICEBERG_CATALOG_CONTROLLER_HPP