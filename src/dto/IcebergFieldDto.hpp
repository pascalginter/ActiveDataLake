#ifndef ICEBERG_FIELD_DTO_HPP
#define ICEBERG_FIELD_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergTypeDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergFieldDto : public oatpp::DTO {
    DTO_INIT(IcebergFieldDto, DTO);

    DTO_FIELD(Int32, id, "id");
    DTO_FIELD(String, name, "name");
    DTO_FIELD(Boolean, required, "required");
    // Sadly, OneOf seems to be an imagination of chatgpt
    DTO_FIELD(Any, type, "type");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_FIELD_DTO_HPP