#ifndef ICEBERG_SNAPSHOT_DTO_HPP
#define ICEBERG_SNAPSHOT_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergSnapshotSummary.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergSnapshotDto : public oatpp::DTO {
    DTO_INIT(IcebergSnapshotDto, DTO)

    DTO_FIELD(Int32, snapshotId, "snapshot-id") = 0;
    DTO_FIELD(Int32, sequenceNumber, "sequence-number") = 0;
    DTO_FIELD(Int64, timestampMs, "timestamp-ms") = 0l;
    DTO_FIELD(Object<IcebergSnapshotSummaryDto>, summary, "summary") = IcebergSnapshotSummaryDto::createShared();
    DTO_FIELD(String, manifestList, "manifest-list") = "http://localhost:8000/manifest-list/list.avro";
    DTO_FIELD(Int32, schemaId, "schema-id") = 0;
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_SNAPSHOT_DTO_HPP