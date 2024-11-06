#ifndef ICEBERG_PARTITION_FIELD_DTO_HPP
#define ICEBERG_PARTITION_FIELD_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergPartitionFieldDto : public oatpp::DTO {

    DTO_INIT(IcebergPartitionFieldDto, DTO)

    DTO_FIELD(Int32, sourceId, "source-id");
    DTO_FIELD(Int32, fieldId, "field-id");
    DTO_FIELD(String, name, "name");
    DTO_FIELD(String, transform, "transform");
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_PARTITION_FIELD_DTO_HPP