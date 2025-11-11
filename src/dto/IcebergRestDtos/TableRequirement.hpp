#ifndef ICEBERG_REST_TABLE_REQUIREMENT_HPP
#define ICEBERG_REST_TABLE_REQUIREMENT_HPP

#include <string>
#include <nlohmann/json.hpp>

struct TableRequirement {
    std::string type;
    std::string ref;
    std::string uuid;
    int64_t snapshotId = 0;  // corresponds to "snapshot-id"

    // JSON serialization
    friend void to_json(nlohmann::json& j, const TableRequirement& r) {
        j = {
            {"type", r.type},
            {"ref", r.ref},
            {"snapshot-id", r.snapshotId},
            {"uuid", r.uuid}
        };
    }

    friend void from_json(const nlohmann::json& j, TableRequirement& r) {
        if (j.contains("type")) r.type = j["type"];
        if (j.contains("ref")) r.ref = j["ref"];
        if (j.contains("snapshot-id")) r.snapshotId = j["snapshot-id"];
        if (j.contains("uuid")) r.uuid = j["uuid"];
    }
};

#endif // ICEBERG_REST_TABLE_REQUIREMENT_HPP
