#ifndef ICEBERG_SNAPSHOT_SUMMARY_HPP
#define ICEBERG_SNAPSHOT_SUMMARY_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class IcebergSnapshotSummaryDto : public oatpp::DTO {
    DTO_INIT(IcebergSnapshotSummaryDto, DTO)

    DTO_FIELD(String, operation, "operation") = "append";
};

#include OATPP_CODEGEN_END(DTO)

#endif // ICEBERG_SNAPSHOT_SUMMARY_HPP