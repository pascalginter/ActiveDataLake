#ifndef ICEBERG_LIST_TABLES_RESPONSE_HPP
#define ICEBERG_LIST_TABLES_RESPONSE_HPP

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include "TableIdentifier.hpp"

struct ListTablesResponse {
    std::string nextPageToken;
    std::vector<TableIdentifier> identifiers;
};

// JSON serialization
inline void to_json(nlohmann::json& j, const ListTablesResponse& r) {
    j = {
        {"next-page-token", r.nextPageToken},
        {"identifiers", r.identifiers}
    };
}

inline void from_json(const nlohmann::json& j, ListTablesResponse& r) {
    j.at("next-page-token").get_to(r.nextPageToken);
    j.at("identifiers").get_to(r.identifiers);
}

#endif // ICEBERG_LIST_TABLES_RESPONSE_HPP
