#ifndef ICEBERG_REST_TABLE_UPDATE_HPP
#define ICEBERG_REST_TABLE_UPDATE_HPP

#include <string>
#include <nlohmann/json.hpp>
#include "../IcebergSnapshot.hpp" // Assumes IcebergSnapshot struct with JSON (de)serialization is defined there

struct TableUpdate {
    std::string action;
    IcebergSnapshot snapshot;

    friend void to_json(nlohmann::json& j, const TableUpdate& u) {
        j = {
            {"action", u.action},
            {"snapshot", u.snapshot}
        };
    }

    friend void from_json(const nlohmann::json& j, TableUpdate& u) {
        if (j.contains("action")) j.at("action").get_to(u.action);
        if (j.contains("snapshot")) j.at("snapshot").get_to(u.snapshot);
    }
};

#endif // ICEBERG_REST_TABLE_UPDATE_HPP
