#ifndef ICEBERG_REST_TABLE_REQUIREMENT_DTO_HPP
#define ICEBERG_REST_TABLE_REQUIREMENT_DTO_HPP

#include "TableIdentifierDto.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class TableRequirementDto final : public oatpp::DTO {
    DTO_INIT(TableRequirementDto, DTO)

    DTO_FIELD(String, type);
    DTO_FIELD(String, ref);
    DTO_FIELD(Int64, snapshotId, "snapshot-id");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_TABLE_REQUIREMENT_DTO_HPP