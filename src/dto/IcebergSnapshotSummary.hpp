#ifndef ICEBERG_SNAPSHOT_SUMMARY_HPP
#define ICEBERG_SNAPSHOT_SUMMARY_HPP

#include <string>
#include <nlohmann/json.hpp>

struct IcebergSnapshotSummary {
    std::string operation = "append";
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergSnapshotSummary& s) {
    j = {
        {"operation", s.operation}
    };
}

inline void from_json(const nlohmann::json& j, IcebergSnapshotSummary& s) {
    j.at("operation").get_to(s.operation);
}

#endif // ICEBERG_SNAPSHOT_SUMMARY_HPP
