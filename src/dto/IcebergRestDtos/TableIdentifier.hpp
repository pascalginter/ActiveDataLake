#ifndef ICEBERG_REST_TABLE_IDENTIFIER_HPP
#define ICEBERG_REST_TABLE_IDENTIFIER_HPP

#include <string>
#include <vector>
#include <nlohmann/json.hpp>

struct TableIdentifier {
    std::vector<std::string> nspace = {"tpch"};  // corresponds to "namespace"
    std::string name = "lineitem";
};

// JSON serialization
inline void to_json(nlohmann::json& j, const TableIdentifier& t) {
    j = {
        {"namespace", t.nspace},
        {"name", t.name}
    };
}

inline void from_json(const nlohmann::json& j, TableIdentifier& t) {
    if (j.contains("namespace")) j.at("namespace").get_to(t.nspace);
    if (j.contains("name")) j.at("name").get_to(t.name);
}

#endif // ICEBERG_REST_TABLE_IDENTIFIER_HPP
