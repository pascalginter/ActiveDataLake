#ifndef ICEBERG_SNAPSHOT_HPP
#define ICEBERG_SNAPSHOT_HPP

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>
#include "IcebergSnapshotSummary.hpp"

struct IcebergSnapshot {
    int32_t snapshotId = 0;
    int32_t sequenceNumber = 0;
    int64_t timestampMs = 0;
    IcebergSnapshotSummary summary;
    std::string manifestList = "http://localhost:8000/manifest-list/list.avro";
    int32_t schemaId = 0;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergSnapshot& s) {
    j = {
        {"snapshot-id", s.snapshotId},
        {"sequence-number", s.sequenceNumber},
        {"timestamp-ms", s.timestampMs},
        {"summary", s.summary},
        {"manifest-list", s.manifestList},
        {"schema-id", s.schemaId}
    };
}

inline void from_json(const nlohmann::json& j, IcebergSnapshot& s) {
    j.at("snapshot-id").get_to(s.snapshotId);
    j.at("sequence-number").get_to(s.sequenceNumber);
    j.at("timestamp-ms").get_to(s.timestampMs);
    j.at("summary").get_to(s.summary);
    j.at("manifest-list").get_to(s.manifestList);
    j.at("schema-id").get_to(s.schemaId);
}

#endif // ICEBERG_SNAPSHOT_HPP
