#ifndef ICEBERG_SORT_FIELD_HPP
#define ICEBERG_SORT_FIELD_HPP

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>

struct IcebergSortField {
    std::string transform;
    int32_t sourceId;
    std::string direction;
    std::string nullOrder;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergSortField& f) {
    j = {
        {"transform", f.transform},
        {"source-id", f.sourceId},
        {"direction", f.direction},
        {"null-order", f.nullOrder}
    };
}

inline void from_json(const nlohmann::json& j, IcebergSortField& f) {
    f.transform = j["transform"];
    f.sourceId = j["source-id"];
    f.direction = j["direction"];
    f.nullOrder = j["null-order"];
}

#endif // ICEBERG_SORT_FIELD_HPP
