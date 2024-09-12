#ifndef ICEBERG_SORT_ORDER_DTO
#define ICEBERG_SORT_ORDER_DTO

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergSortOrderDto : public oatpp::DTO {
    DTO_INIT(IcebergSortOrderDto, DTO);
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_SORT_ORDER_DTO