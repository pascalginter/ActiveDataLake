#ifndef ICEBERG_REST_UPDATE_TABLE_RESPONSE_HPP
#define ICEBERG_REST_UPDATE_TABLE_RESPONSE_HPP

#include <string>
#include <nlohmann/json.hpp>
#include "../IcebergMetadata.hpp"

struct UpdateTableResponse {
    std::string metadataLocation;
    IcebergMetadata metadata;
};

// JSON serialization
inline void to_json(nlohmann::json& j, const UpdateTableResponse& r) {
    j = {
        {"metadata-location", r.metadataLocation},
        {"metadata", r.metadata}
    };
}

inline void from_json(const nlohmann::json& j, UpdateTableResponse& r) {
    r.metadataLocation = j["metadata-location"];
    r.metadata = j["metadata"];
}

#endif // ICEBERG_REST_UPDATE_TABLE_RESPONSE_HPP
