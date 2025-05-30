#ifndef ICEBERG_STRUCT_TYPE_HPP
#define ICEBERG_STRUCT_TYPE_HPP

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include "IcebergField.hpp"

struct IcebergStructType {
    std::string type = "struct";
    std::vector<IcebergField> fields;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergStructType& s) {
    j = {
        {"type", s.type},
        {"fields", s.fields}
    };
}

inline void from_json(const nlohmann::json& j, IcebergStructType& s) {
    j.at("type").get_to(s.type);
    j.at("fields").get_to(s.fields);
}

#endif // ICEBERG_STRUCT_TYPE_HPP
