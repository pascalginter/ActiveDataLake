#ifndef ICEBERG_REST_CONFIG_HPP
#define ICEBERG_REST_CONFIG_HPP

#include <string>
#include <vector>
#include <nlohmann/json.hpp>

struct Config {
    std::vector<std::string> endpoints = {
        "GET /v1/{prefix}/namespaces",
        "POST /v1/{prefix}/namespaces",
        "GET /v1/{prefix}/namespaces/{namespace}",
        "DELETE /v1/{prefix}/namespaces/{namespace}",
        "POST /v1/{prefix}/namespaces/{namespace}/properties",
        "GET /v1/{prefix}/namespaces/{namespace}/tables",
        "POST /v1/{prefix}/namespaces/{namespace}/tables",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "POST /v1/{prefix}/namespaces/{namespace}/register",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
        "POST /v1/{prefix}/tables/rename",
        "POST /v1/{prefix}/transactions/commit"
    };

    nlohmann::json defaults = nlohmann::json::object();
    nlohmann::json overrides = nlohmann::json::object();
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const Config& c) {
    j = {
        {"endpoints", c.endpoints},
        {"defaults", c.defaults},
        {"overrides", c.overrides}
    };
}

inline void from_json(const nlohmann::json& j, Config& c) {
    c.endpoints = j["endpoints"];
    if (j.contains("defaults")) {
        c.defaults = j["defaults"];
    }
    if (j.contains("overrides")) {
        c.overrides = j["overrides"];
    }
}

#endif // ICEBERG_REST_CONFIG_HPP
