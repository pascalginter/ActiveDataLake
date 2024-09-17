#ifndef ICEBERG_REST_CONFIG_DTO_HPP
#define ICEBERG_REST_CONFIG_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class ConfigDto : public oatpp::DTO {
    DTO_INIT(ConfigDto, DTO)

    DTO_FIELD(String, description, "description") = "";
    DTO_FIELD()
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_CONFIG_DTO_HPP