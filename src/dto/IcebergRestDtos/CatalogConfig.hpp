#ifndef ICEBERG_REST_CONFIG_HPP
#define ICEBERG_REST_CONFIG_HPP

#include <string>
#include <vector>
#include <nlohmann/json.hpp>

struct Config {
    std::vector<std::string> endpoints = {
        "GET /v1/{prefix}/namespaces",
        "GET /v1/{prefix}/namespaces/{namespace}",
        "GET /v1/{prefix}/namespaces/{namespace}/tables"
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
    j.at("endpoints").get_to(c.endpoints);
    if (j.contains("defaults")) {
        c.defaults = j.at("defaults");
    }
    if (j.contains("overrides")) {
        c.overrides = j.at("overrides");
    }
}

#endif // ICEBERG_REST_CONFIG_HPP
