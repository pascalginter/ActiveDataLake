#ifndef ICEBERG_REST_LIST_TABLES_RESPONSE_DTO_HPP
#define ICEBERG_REST_LIST_TABLES_RESPONSE_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "TableIdentifierDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class ListTablesResponseDto : public oatpp::DTO {
    DTO_INIT(ListTablesResponseDto, DTO)

    DTO_FIELD(String, nextPageToken, "next-page-token");
    DTO_FIELD(List<Object<TableIdentifierDto>>, identifiers, "identifiers") = {};
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_LIST_TABLES_RESPONSE_DTO_HPP