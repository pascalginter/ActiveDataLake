#ifndef ICEBERG_PARTITION_SPEC_HPP
#define ICEBERG_PARTITION_SPEC_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergPartitionSpecDto : public oatpp::DTO {

    DTO_INIT(IcebergPartitionSpecDto, DTO)

    DTO_FIELD_INFO(blob) {
        info->description = "blob";
    }
    DTO_FIELD(String, blob);

};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_PARTITION_SPEC_HPP