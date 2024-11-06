#ifndef ICEBERG_SCHEMA_DTO_HPP
#define ICEBERG_SCHEMA_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "IcebergStructTypeDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergSchemaDto : public IcebergStructTypeDto {
    DTO_INIT(IcebergSchemaDto, IcebergStructTypeDto);

    DTO_FIELD(Int32, schemaId, "schemaId") = 0;
    // 1 optional field
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_SCHEMA_DTO_HPP