#ifndef ICEBERG_REST_LOAD_TABLE_DTO_HPP
#define ICEBERG_REST_LOAD_TABLE_DTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include "../IcebergMetadataDto.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class LoadTableResponseDto : public oatpp::DTO {
    DTO_INIT(LoadTableResponseDto, DTO)

    DTO_FIELD(String, metadataLocation, "metadata-location") = "http://localhost:8000/metadata/metadata.json";
    DTO_FIELD(Object<IcebergMetadataDto>, metadata, "metadata") = IcebergMetadataDto::createShared();
    DTO_FIELD(Fields<Any>, config, "config") = {};
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_LOAD_TABLE_DTO_HPP