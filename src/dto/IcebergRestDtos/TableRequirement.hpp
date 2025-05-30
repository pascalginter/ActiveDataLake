#ifndef ICEBERG_REST_TABLE_REQUIREMENT_HPP
#define ICEBERG_REST_TABLE_REQUIREMENT_HPP

#include <string>
#include <nlohmann/json.hpp>

struct TableRequirement {
    std::string type;
    std::string ref;
    int64_t snapshotId = 0;  // corresponds to "snapshot-id"

    // JSON serialization
    friend void to_json(nlohmann::json& j, const TableRequirement& r) {
        j = {
            {"type", r.type},
            {"ref", r.ref},
            {"snapshot-id", r.snapshotId}
        };
    }

    friend void from_json(const nlohmann::json& j, TableRequirement& r) {
        if (j.contains("type")) j.at("type").get_to(r.type);
        if (j.contains("ref")) j.at("ref").get_to(r.ref);
        if (j.contains("snapshot-id")) j.at("snapshot-id").get_to(r.snapshotId);
    }
};

#endif // ICEBERG_REST_TABLE_REQUIREMENT_HPP
