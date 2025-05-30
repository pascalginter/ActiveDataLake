#ifndef ICEBERG_PARTITION_FIELD_HPP
#define ICEBERG_PARTITION_FIELD_HPP

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>

struct IcebergPartitionField {
    int32_t sourceId;
    int32_t fieldId;
    std::string name;
    std::string transform;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergPartitionField& f) {
    j = {
        {"source-id", f.sourceId},
        {"field-id", f.fieldId},
        {"name", f.name},
        {"transform", f.transform}
    };
}

inline void from_json(const nlohmann::json& j, IcebergPartitionField& f) {
    j.at("source-id").get_to(f.sourceId);
    j.at("field-id").get_to(f.fieldId);
    j.at("name").get_to(f.name);
    j.at("transform").get_to(f.transform);
}

#endif // ICEBERG_PARTITION_FIELD_HPP