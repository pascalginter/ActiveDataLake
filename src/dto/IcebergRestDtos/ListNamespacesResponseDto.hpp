#ifndef ICEBERG_LIST_NAMESPACES_RESPONSE_DTO_HPP
#define ICEBERG_LIST_NAMESPACES_RESPONSE_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class ListNamespacesResponseDto : public oatpp::DTO {
    DTO_INIT(ListNamespacesResponseDto, DTO)

    DTO_FIELD(String, nextPageToken, "next-page-token");
    DTO_FIELD(List<List<String>>, namespaces, "namespaces") = {
            {"tpch", "sf-1"}
    };
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_LIST_NAMESPACES_RESPONSE_DTO_HPP