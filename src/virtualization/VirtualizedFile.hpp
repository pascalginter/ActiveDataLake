#ifndef VirtualizedFile_H
#define VirtualizedFile_H

#include <string>
#include <memory>

#include "../controller/S3InterfaceUtils.hpp"
#include "oatpp/web/protocol/http/outgoing/BufferBody.hpp"


class VirtualizedFile {
public:
    virtual ~VirtualizedFile() = default;

    virtual size_t size() = 0;
    virtual std::shared_ptr<std::string> getRange(S3InterfaceUtils::ByteRange byteRange) = 0;

    static std::shared_ptr<VirtualizedFile> createFileAbstraction(std::string path);
};

#endif