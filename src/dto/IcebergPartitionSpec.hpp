#ifndef ICEBERG_PARTITION_SPEC_HPP
#define ICEBERG_PARTITION_SPEC_HPP

#include <vector>
#include <cstdint>
#include <nlohmann/json.hpp>
#include "IcebergPartitionField.hpp"

struct IcebergPartitionSpec {
    int32_t specId = 0;
    std::vector<IcebergPartitionField> fields;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergPartitionSpec& s) {
    j = {
        {"spec-id", s.specId},
        {"fields", s.fields}
    };
}

inline void from_json(const nlohmann::json& j, IcebergPartitionSpec& s) {
    j.at("spec-id").get_to(s.specId);
    j.at("fields").get_to(s.fields);
}

#endif // ICEBERG_PARTITION_SPEC_HPP
