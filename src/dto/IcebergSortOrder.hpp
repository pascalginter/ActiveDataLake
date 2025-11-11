#ifndef ICEBERG_SORT_ORDER_HPP
#define ICEBERG_SORT_ORDER_HPP

#include <cstdint>
#include <vector>
#include <nlohmann/json.hpp>
#include "IcebergSortField.hpp"

struct IcebergSortOrder {
    int32_t orderId = 0;
    std::vector<IcebergSortField> fields;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergSortOrder& o) {
    j = {
        {"order-id", o.orderId},
        {"fields", o.fields}
    };
}

inline void from_json(const nlohmann::json& j, IcebergSortOrder& o) {
    o.orderId = j["order-id"];
    o.fields = j["fields"];
}

#endif // ICEBERG_SORT_ORDER_HPP
