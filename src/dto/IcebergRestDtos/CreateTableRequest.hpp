#ifndef ICEBERG_REST_CREATE_TABLE_REQUEST_HPP
#define ICEBERG_REST_CREATE_TABLE_REQUEST_HPP

#include <string>
#include <optional>
#include <nlohmann/json.hpp>

#include "../IcebergSchema.hpp"
#include "../IcebergPartitionSpec.hpp"
#include "../IcebergSortOrder.hpp"

struct CreateTableRequest {
    std::string name;
    std::optional<std::string> location;
    IcebergSchema schema;
    std::optional<IcebergPartitionSpec> partitionSpec;
    std::optional<IcebergSortOrder> writeOrder;
    std::optional<bool> stageCreate = false;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const CreateTableRequest& r) {
    j = {
        {"name", r.name},
        {"schema", r.schema},
    };
    if (r.partitionSpec) j["partition-spec"] = *r.partitionSpec;
    if (r.writeOrder) j["write-order"] = *r.writeOrder;
    if (r.stageCreate) j["stage-create"] = *r.stageCreate;
}

inline void from_json(const nlohmann::json& j, CreateTableRequest& r) {
    j.at("name").get_to(r.name);
    j.at("schema").get_to(r.schema);
    if (j.contains("location")) r.location = j["location"];
    if (j.contains("partition-spec")) r.partitionSpec = j["partition-spec"];
    if (j.contains("write-order")) r.writeOrder = j["write-order"];
    if (j.contains("stage-create")) r.stageCreate = j["stage-create"];
}

#endif // ICEBERG_REST_CREATE_TABLE_REQUEST_HPP
