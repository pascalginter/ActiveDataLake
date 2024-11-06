#ifndef ICEBERG_REST_CONFIG_DTO_HPP
#define ICEBERG_REST_CONFIG_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class ConfigDto : public oatpp::DTO {
    DTO_INIT(ConfigDto, DTO)

    DTO_FIELD(List<String>, endpoints, "endpoints") = {
            "GET /v1/{prefix}/namespaces",
            "GET /v1/{prefix}/namespaces/{namespace}"
            "GET /v1/{prefix}/namespaces/{namespace}/tables"
    };

    DTO_FIELD(Fields<Any>, defaults, "defaults") = {};
    DTO_FIELD(Fields<Any>, overrides, "overrides") = {};
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_CONFIG_DTO_HPP