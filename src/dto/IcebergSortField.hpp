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
    j.at("transform").get_to(f.transform);
    j.at("source-id").get_to(f.sourceId);
    j.at("direction").get_to(f.direction);
    j.at("null-order").get_to(f.nullOrder);
}

#endif // ICEBERG_SORT_FIELD_HPP
