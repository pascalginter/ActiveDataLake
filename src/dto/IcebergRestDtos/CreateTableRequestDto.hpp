#ifndef ICEBERG_REST_CREATE_TABLE_REQUEST_DTO_HPP
#define ICEBERG_REST_CREATE_TABLE_REQUEST_DTO_HPP

#include "../IcebergPartitionSpecDto.hpp"
#include "../IcebergSortOrderDto.hpp"
#include "../IcebergSchemaDto.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class CreateTableRequestDto final : public oatpp::DTO {
    DTO_INIT(CreateTableRequestDto, DTO)

    DTO_FIELD(String, name);
    DTO_FIELD(String, location);
    DTO_FIELD(Object<IcebergSchemaDto>, schema);
    DTO_FIELD(Object<IcebergPartitionSpecDto>, partitionSpec, "partition-spec");
    DTO_FIELD(Object<IcebergSortOrderDto>, writeOrder, "write-order");
    DTO_FIELD(Boolean, stageCreate, "stage-create");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_CREATE_TABLE_REQUEST_DTO_HPP