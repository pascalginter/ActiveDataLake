#ifndef ICEBERG_REST_COMMIT_TABLE_REQUEST_DTO_HPP
#define ICEBERG_REST_COMMIT_TABLE_REQUEST_DTO_HPP

#include "TableIdentifierDto.hpp"
#include "TableRequirementDto.hpp"
#include "TableUpdateDto.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class CommitTableRequestDto final : public oatpp::DTO {
    DTO_INIT(CommitTableRequestDto, DTO)

    DTO_FIELD(Object<TableIdentifierDto>, identifier);
    DTO_FIELD(List<Object<TableRequirementDto>>, requirements);
    DTO_FIELD(List<Object<TableUpdateDto>>, updates);
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_REST_COMMIT_TABLE_REQUEST_DTO_HPP