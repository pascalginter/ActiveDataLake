#ifndef ICEBERG_STRUCT_TYPE_DTO_HPP
#define ICEBERG_STRUCT_TYPE_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergFieldDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergStructTypeDto : public oatpp::DTO {
    DTO_INIT(IcebergStructTypeDto, DTO);

    DTO_FIELD(String, type, "type") = "struct";
    DTO_FIELD(List<Object<IcebergFieldDto>>, fields, "fields");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_STRUCT_TYPE_DTO_HPP