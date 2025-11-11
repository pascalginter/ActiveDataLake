#ifndef ICEBERG_REST_CREATE_NAMESPACE_REQUEST_HPP
#define ICEBERG_REST_CREATE_NAMESPACE_REQUEST_HPP

#include <vector>
#include <string>
#include <nlohmann/json.hpp>

struct CreateNamespaceRequest {
    std::vector<std::string> namespaces; // Note: JSON key is "namespace"
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const CreateNamespaceRequest& r) {
    j = { {"namespace", r.namespaces} };
}

inline void from_json(const nlohmann::json& j, CreateNamespaceRequest& r) {
    r.namespaces = j["namespace"];
}

#endif // ICEBERG_REST_CREATE_NAMESPACE_REQUEST_HPP
