
#ifndef CRUD_BLOBDTO_HPP
#define CRUD_BLOBDTO_HPP

#include "oatpp/macro/codegen.hpp"
#include "oatpp/Types.hpp"

#include OATPP_CODEGEN_BEGIN(DTO)

class BlobDto : public oatpp::DTO {

    DTO_INIT(BlobDto, DTO)

    DTO_FIELD_INFO(blob) {
        info->description = "blob";
    }
    DTO_FIELD(String, blob);

};

#include OATPP_CODEGEN_END(DTO)

#endif //CRUD_BLOBDTO_HPP