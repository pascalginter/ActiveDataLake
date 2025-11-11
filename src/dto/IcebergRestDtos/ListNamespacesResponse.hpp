#ifndef ICEBERG_LIST_NAMESPACES_RESPONSE_HPP
#define ICEBERG_LIST_NAMESPACES_RESPONSE_HPP

#include <string>
#include <vector>
#include <nlohmann/json.hpp>

struct ListNamespacesResponse {
    std::string nextPageToken;
    std::vector<std::vector<std::string>> namespaces = {};
};

// JSON serialization
inline void to_json(nlohmann::json& j, const ListNamespacesResponse& r) {
    j = {
        {"next-page-token", r.nextPageToken},
        {"namespaces", r.namespaces}
    };
}

inline void from_json(const nlohmann::json& j, ListNamespacesResponse& r) {
    r.nextPageToken = j["next-page-token"];
    r.namespaces = j["namespaces"];
}

#endif // ICEBERG_LIST_NAMESPACES_RESPONSE_HPP
