#ifndef ICEBERG_REST_TABLE_IDENTIFIER_DTO_HPP
#define ICEBERG_REST_TABLE_IDENTIFIER_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class TableIdentifierDto : public oatpp::DTO {
    DTO_INIT(TableIdentifierDto, DTO)

    DTO_FIELD(List<String>, nspace, "namespace") = {"tpch"};
    DTO_FIELD(String, name, "name") = "lineitem";
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_TABLE_IDENTIFIER_DTO_HPP