#ifndef ICEBERG_REST_COMMIT_TABLE_REQUEST_HPP
#define ICEBERG_REST_COMMIT_TABLE_REQUEST_HPP

#include <vector>
#include <nlohmann/json.hpp>
#include "TableIdentifier.hpp"
#include "TableRequirement.hpp"
#include "TableUpdate.hpp"

struct CommitTableRequest {
    TableIdentifier identifier;
    std::vector<TableRequirement> requirements;
    std::vector<TableUpdate> updates;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const CommitTableRequest& r) {
    j = {
        {"identifier", r.identifier},
        {"requirements", r.requirements},
        {"updates", r.updates}
    };
}

inline void from_json(const nlohmann::json& j, CommitTableRequest& r) {
    j.at("identifier").get_to(r.identifier);
    j.at("requirements").get_to(r.requirements);
    j.at("updates").get_to(r.updates);
}

#endif // ICEBERG_REST_COMMIT_TABLE_REQUEST_HPP
