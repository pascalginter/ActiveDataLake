#ifndef ICEBERG_REST_TABLE_METADATA_HPP
#define ICEBERG_REST_TABLE_METADATA_HPP

#include <string>
#include <nlohmann/json.hpp>

struct TableMetadata {
    int formatVersion = 1;  // corresponds to "format-version"
    std::string tableUuid = "a4b80d01-cd28-4fb4-bc50-b232f9566c16";  // "table-uuid"
    std::string location = "http://localhost:8000/metadata/lineitem.json";

    // JSON serialization
    friend void to_json(nlohmann::json& j, const TableMetadata& t) {
        j = {
            {"format-version", t.formatVersion},
            {"table-uuid", t.tableUuid},
            {"location", t.location}
        };
    }

    friend void from_json(const nlohmann::json& j, TableMetadata& t) {
        if (j.contains("format-version")) j.at("format-version").get_to(t.formatVersion);
        if (j.contains("table-uuid")) j.at("table-uuid").get_to(t.tableUuid);
        if (j.contains("location")) j.at("location").get_to(t.location);
    }
};

#endif // ICEBERG_REST_TABLE_METADATA_HPP
