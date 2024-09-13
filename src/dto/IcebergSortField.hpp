#ifndef ICEBERG_SORT_FIELD_DTO_HPP
#define ICEBERG_SORT_FIELD_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergSortFieldDto : public oatpp::DTO {
    DTO_INIT(IcebergSortFieldDto, DTO)

    DTO_FIELD(String, transform, "transform");
    DTO_FIELD(Int32, sourceId, "source-id");
    DTO_FIELD(String, direction, "direction");
    DTO_FIELD(String, nullOrder, "null-order");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_SORT_FIELD_DTO_HPP