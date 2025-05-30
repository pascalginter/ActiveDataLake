#ifndef ICEBERG_REST_CREATE_NAMESPACE_REQUEST_DTO_HPP
#define ICEBERG_REST_CREATE_NAMESPACE_REQUEST_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class CreateNamespaceRequestDto final : public oatpp::DTO {
    DTO_INIT(CreateNamespaceRequestDto, DTO)

    DTO_FIELD(List<String>, namespaces, "namespace") = {};
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_CREATE_NAMESPACE_REQUEST_DTO_HPP