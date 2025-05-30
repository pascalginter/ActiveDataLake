#ifndef ICEBERG_FIELD_HPP
#define ICEBERG_FIELD_HPP

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>


struct IcebergField {
    int32_t id;
    std::string name;
    bool required;
    std::string type;
};

// JSON (de)serialization
inline void to_json(nlohmann::json& j, const IcebergField& f) {
    j = {
        {"id", f.id},
        {"name", f.name},
        {"required", f.required},
        {"type", f.type}
    };
}

inline void from_json(const nlohmann::json& j, IcebergField& f) {
    j.at("id").get_to(f.id);
    j.at("name").get_to(f.name);
    j.at("required").get_to(f.required);
    j.at("type").get_to(f.type);
}

#endif // ICEBERG_FIELD_HPP
