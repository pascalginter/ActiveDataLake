#ifndef ICEBERG_METADATA_DTO_HPP
#define ICEBERG_METADATA_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergSchemaDto.hpp"
#include "IcebergPartitionSpecDto.hpp"
#include "IcebergSortOrderDto.hpp"
#include "IcebergSnapshotDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergMetadataDto : public oatpp::DTO {

    DTO_INIT(IcebergMetadataDto, DTO)

    DTO_FIELD(Int32, formatVersion, "format-version") = 2;
    DTO_FIELD(String, tableUUID, "table-uuid") = "b0dc1444-6864-46f2-b41d-1876b96d1d3f";
    DTO_FIELD(String, location, "location") = "'http://localhost:8000/'";
    DTO_FIELD(Int64, lastSequenceNumber, "last-sequence-number") = 1;
    DTO_FIELD(Int64, lastUpdatedMs, "last-updated-ms") = 1;
    DTO_FIELD(Int64, lastColumnId, "last-column-id") = 80;
    DTO_FIELD(List<Object<IcebergSchemaDto>>, schemas, "schemas") = {IcebergSchemaDto::createShared()};
    DTO_FIELD(Int32, currentSchemaId, "current-schema-id") = 0;
    DTO_FIELD(List<Object<IcebergPartitionSpecDto>>, partitionSpecs, "partition-specs") = {IcebergPartitionSpecDto::createShared()};
    DTO_FIELD(Int32, defaultSpecId, "default-spec-id") = 0;
    DTO_FIELD(Int32, lastPartitionId, "last-partition-id") = 0;
    // properties (optional)
    DTO_FIELD(Int64, currentSnapshotId, "current-snapshot-id") = 0l;
    DTO_FIELD(List<Object<IcebergSnapshotDto>>, snapshots, "snapshots") = {};
    // snapshot-log (optional)
    // metadata-log (optional)
    DTO_FIELD(List<Object<IcebergSortOrderDto>>, sortOrders, "sort-orders") = {IcebergSortOrderDto::createShared()};
    DTO_FIELD(Int32, defaultSortOrderId, "default-sort-order-id") = 0;
};

#include OATPP_CODEGEN_END(DTO)

#endif //ICEBERG_METADATA_DTO_HPP