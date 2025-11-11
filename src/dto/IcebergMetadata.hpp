#ifndef ICEBERG_METADATA_HPP
#define ICEBERG_METADATA_HPP

#include <string>
#include <vector>
#include <cstdint>
#include <nlohmann/json.hpp>

#include "IcebergSchema.hpp"
#include "IcebergPartitionSpec.hpp"
#include "IcebergSortOrder.hpp"
#include "IcebergSnapshot.hpp"

struct IcebergMetadata {
    int32_t formatVersion = 2;
    std::string tableUUID = "b0dc1444-6864-46f2-b41d-1876b96d1d3f";
    std::string location = ".";
    int64_t lastSequenceNumber = 1;
    int64_t lastUpdatedMs = 1;
    int64_t lastColumnId = 80;
    std::vector<IcebergSchema> schemas;
    int32_t currentSchemaId = 0;
    std::vector<IcebergPartitionSpec> partitionSpecs;
    int32_t defaultSpecId = 0;
    int32_t lastPartitionId = 0;
    int64_t currentSnapshotId = 0;
    std::vector<IcebergSnapshot> snapshots;
    std::vector<IcebergSortOrder> sortOrders;
    int32_t defaultSortOrderId = 0;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergMetadata& m) {
    j = {
        {"format-version", m.formatVersion},
        {"table-uuid", m.tableUUID},
        {"location", m.location},
        {"last-sequence-number", m.lastSequenceNumber},
        {"last-updated-ms", m.lastUpdatedMs},
        {"last-column-id", m.lastColumnId},
        {"schemas", m.schemas},
        {"current-schema-id", m.currentSchemaId},
        {"partition-specs", m.partitionSpecs},
        {"default-spec-id", m.defaultSpecId},
        {"last-partition-id", m.lastPartitionId},
        {"current-snapshot-id", m.currentSnapshotId},
        {"snapshots", m.snapshots},
        {"sort-orders", m.sortOrders},
        {"default-sort-order-id", m.defaultSortOrderId}
    };
}

inline void from_json(const nlohmann::json& j, IcebergMetadata& m) {
    m.formatVersion = j["format-version"];
    m.tableUUID = j["table-uuid"];
    m.location = j["location"];
    m.lastSequenceNumber = j["last-sequence-number"];
    m.lastUpdatedMs = j ["last-updated-ms"];
    m.lastColumnId = j["last-column-id"];
    m.schemas = j["schemas"];
    m.currentSchemaId = j["current-schema-id"];
    m.partitionSpecs = j["partition-specs"];
    m.defaultSpecId = j["default-spec-id"];
    m.lastPartitionId = j["last-partition-id"];
    m.currentSnapshotId = j["current-snapshot-id"];
    m.snapshots = j["snapshot"];
    m.sortOrders = j["sort-orders"];
    m.defaultSortOrderId = j["default-sort-order-id"];
}

#endif // ICEBERG_METADATA_HPP
