#ifndef ICEBERG_REST_TABLE_UPDATE_DTO_HPP
#define ICEBERG_REST_TABLE_UPDATE_DTO_HPP

#include "TableIdentifierDto.hpp"
#include "../IcebergSnapshotDto.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class TableUpdateDto final : public oatpp::DTO {
    DTO_INIT(TableUpdateDto, DTO)

    DTO_FIELD(String, action);
    DTO_FIELD(Object<IcebergSnapshotDto>, snapshot);
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_TABLE_UPDATE_DTO_HPP