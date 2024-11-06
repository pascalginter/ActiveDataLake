#ifndef ICEBERG_REST_TABLE_METADATA_DTO_HPP
#define ICEBERG_REST_TABLE_METADATA_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class TableMetadataDto : public oatpp::DTO {
    DTO_INIT(TableMetadataDto, DTO)

    DTO_FIELD(Int32, formatVersion, "format-version") = 1;
    DTO_FIELD(String, tableUuid, "table-uuid") = "a4b80d01-cd28-4fb4-bc50-b232f9566c16";
    DTO_FIELD(String, location, "location") = "http://localhost:8000/metadata/lineitem.json";
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_TABLE_METADATA_DTO_HPP