#ifndef ICEBERG_FIELD_DTO_HPP
#define ICEBERG_FIELD_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergTypeDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergFieldDto : public oatpp::DTO {
    DTO_INIT(IcebergFieldDto, DTO);

    DTO_FIELD(Int32, id, "id") = 0;
    DTO_FIELD(String, name, "name") = "l_orderkey";
    DTO_FIELD(Boolean, required, "required") = true;
    // Sadly, OneOf seems to be an imagination of chatgpt
    DTO_FIELD(Any, type, "type") = String("int");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_FIELD_DTO_HPP