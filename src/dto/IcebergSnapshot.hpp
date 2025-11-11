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
    s.snapshotId = j["snapshot-id"];
    s.sequenceNumber = j["sequence-number"];
    s.timestampMs = j["timestamp-ms"];
    s.summary = j["summary"];
    s.manifestList = j["manifest-list"];
    s.schemaId = j["schema-id"];
}

#endif // ICEBERG_SNAPSHOT_HPP
