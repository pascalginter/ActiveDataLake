#ifndef ICEBERG_METADATA_DTO_HPP
#define ICEBERG_METADATA_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergSchemaDto.hpp"
#include "IcebergPartitionSpecDto.hpp"
#include "IcebergSortOrderDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergMetadataDto : public oatpp::DTO {

    DTO_INIT(IcebergMetadataDto, DTO)

    DTO_FIELD(Int32, formatVersion, "format-version");
    DTO_FIELD(String, tableUUID, "table-uuid");
    DTO_FIELD(String, location, "location");
    DTO_FIELD(Int64, lastSequenceNumber, "last-sequence-number");
    DTO_FIELD(Int64, lastUpdatedMs, "last-updated-ms");
    DTO_FIELD(Int64, lastColumnId, "last-column-id");
    DTO_FIELD(List<Object<IcebergSchemaDto>>, schmemas, "schemas");
    DTO_FIELD(Int32, currentSchemaId, "current-schema-id");
    DTO_FIELD(List<Object<IcebergPartitionSpecDto>>, partitionSpecs, "partition-specs");
    DTO_FIELD(Int32, defaultIdSpec, "default-id-spec");
    DTO_FIELD(Int32, lastPartitionId, "last-partition-id");
    // 5 Optional Fields
    DTO_FIELD(List<Object<IcebergSortOrderDto>>, sortOrders, "sort-orders");
    DTO_FIELD(Int32, defaultSortOrderId, "default-sort-order-id");
};

#include OATPP_CODEGEN_END(DTO)

#endif //ICEBERG_METADATA_DTO_HPP