#ifndef CRUD_REQUESTDTO_HPP
#define CRUD_REQUESTDTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class RequestDto : public oatpp::DTO {

    DTO_INIT(RequestDto, DTO)

    DTO_FIELD(String, IfMatch);
    DTO_FIELD(String, IfModifiedSince);
    DTO_FIELD(String, IfNoneMatched);
    DTO_FIELD(String, IfUnmodifiedSince);
    DTO_FIELD(String, Range);
    DTO_FIELD(String, SSECustomerAlgorithm);
    DTO_FIELD(String, SSECustomerKey);
    DTO_FIELD(String, SSECustomerKeyMD5);
    DTO_FIELD(String, RequestPayer);
    DTO_FIELD(String, ExpectedBucketOwner);
    DTO_FIELD(String, ChecksumMode);
};

#include OATPP_CODEGEN_END(DTO)

#endif //CRUD_REQUESTDTO_HPP