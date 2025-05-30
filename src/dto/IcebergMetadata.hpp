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
    j.at("format-version").get_to(m.formatVersion);
    j.at("table-uuid").get_to(m.tableUUID);
    j.at("location").get_to(m.location);
    j.at("last-sequence-number").get_to(m.lastSequenceNumber);
    j.at("last-updated-ms").get_to(m.lastUpdatedMs);
    j.at("last-column-id").get_to(m.lastColumnId);
    j.at("schemas").get_to(m.schemas);
    j.at("current-schema-id").get_to(m.currentSchemaId);
    j.at("partition-specs").get_to(m.partitionSpecs);
    j.at("default-spec-id").get_to(m.defaultSpecId);
    j.at("last-partition-id").get_to(m.lastPartitionId);
    j.at("current-snapshot-id").get_to(m.currentSnapshotId);
    j.at("snapshots").get_to(m.snapshots);
    j.at("sort-orders").get_to(m.sortOrders);
    j.at("default-sort-order-id").get_to(m.defaultSortOrderId);
}

#endif // ICEBERG_METADATA_HPP