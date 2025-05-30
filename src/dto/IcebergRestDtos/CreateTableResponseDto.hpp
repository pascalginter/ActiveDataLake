#ifndef ICEBERG_REST_UPDATE_TABLE_RESPONSE_DTO_HPP
#define ICEBERG_REST_UPDATE_TABLE_RESPONSE_DTO_HPP


#include "../IcebergMetadataDto.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class UpdateTableResponseDto final : public oatpp::DTO {
    DTO_INIT(UpdateTableResponseDto, DTO)

    DTO_FIELD(String, metadataLocation, "metadata-location");
    DTO_FIELD(Object<IcebergMetadataDto>, metadata);
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_UPDATE_TABLE_RESPONSE_DTO_HPP