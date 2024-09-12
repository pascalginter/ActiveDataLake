#ifndef ICEBERG_TYPE_DTO_HPP
#define ICEBERG_TYPE_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergTypeDto : public oatpp::DTO {
    DTO_INIT(IcebergTypeDto, DTO)
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_TYPE_DTO_HPP