#ifndef ICEBERG_REST_LOAD_TABLE_HPP
#define ICEBERG_REST_LOAD_TABLE_HPP

#include <string>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include "../IcebergMetadata.hpp"

struct LoadTableResponse {
    std::string metadataLocation = "http://localhost:8000/metadata/metadata.json";
    IcebergMetadata metadata;
    std::unordered_map<std::string, nlohmann::json> config; // flexible key-value config
};

// JSON serialization
inline void to_json(nlohmann::json& j, const LoadTableResponse& r) {
    j = {
        {"metadata-location", r.metadataLocation},
        {"metadata", r.metadata},
        {"config", r.config}
    };
}

inline void from_json(const nlohmann::json& j, LoadTableResponse& r) {
    if (j.contains("metadata-location")) j.at("metadata-location").get_to(r.metadataLocation);
    if (j.contains("metadata")) j.at("metadata").get_to(r.metadata);
    if (j.contains("config")) j.at("config").get_to(r.config);
}

#endif // ICEBERG_REST_LOAD_TABLE_HPP
