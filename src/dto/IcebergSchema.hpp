#ifndef ICEBERG_SCHEMA_HPP
#define ICEBERG_SCHEMA_HPP

#include <cstdint>
#include <nlohmann/json.hpp>
#include "IcebergStructType.hpp"

struct IcebergSchema : public IcebergStructType {
    int32_t schemaId = 0;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergSchema& s) {
    // Serialize base class first
    to_json(j, static_cast<const IcebergStructType&>(s));
    j["schema-id"] = s.schemaId;
}

inline void from_json(const nlohmann::json& j, IcebergSchema& s) {
    // Deserialize base class first
    from_json(j, static_cast<IcebergStructType&>(s));
    if (j.contains("schema-id")) {
        j.at("schema-id").get_to(s.schemaId);
    }
}

#endif // ICEBERG_SCHEMA_HPP
