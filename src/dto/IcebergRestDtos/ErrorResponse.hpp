#ifndef ICEBERG_REST_ERROR_RESPONSE_HPP
#define ICEBERG_REST_ERROR_RESPONSE_HPP

#include <string>
#include <nlohmann/json.hpp>

struct ErrorResponse {
    std::string message;
    std::string type;
    int code;
};

// JSON serialization
inline void to_json(nlohmann::json& j, const ErrorResponse& r) {
    j = {"error", {
        {"message", r.message},
        {"type", r.type},
        {"code", r.code}
    }};
}

inline void from_json(const nlohmann::json& j, ErrorResponse& r) {
    r.message = j["error"]["message"];
    r.type = j["error"]["type"];
    r.code = j["error"]["code"];
}

#endif // ICEBERG_REST_ERROR_RESPONSE_HPP
