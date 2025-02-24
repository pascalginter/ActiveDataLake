#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/S3Errors.h>

#include "VirtualizedFile.hpp"

class RemoteFile : public VirtualizedFile {
    std::string key;
    constexpr static std::string bucket = "adl-tpch";
    constexpr static size_t increment = 16 * (1 << 20);
    static thread_local std::shared_ptr<std::string> result;
    static thread_local Aws::S3::S3Client client;
public:
    explicit RemoteFile(const std::string& p) : key(p + ".parquet"){
        std::cout << "created remote file" << std::endl;
    }

    size_t size() override {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(key);
        if (const auto outcome = client.HeadObject(request); outcome.IsSuccess()) {
            return outcome.GetResult().GetContentLength();
        } else {
            std::cout << outcome.GetError().GetMessage().c_str() << std::endl;
            exit(1);
        }
    }

    std::shared_ptr<std::string> getRange(S3InterfaceUtils::ByteRange byteRange) override {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(key);
        request.SetRange(byteRange.toRangeString());
        if (const auto outcome = client.GetObject(request); outcome.IsSuccess()) {
            auto& stream = outcome.GetResult().GetBody();
            result->reserve(byteRange.size());
            result->assign(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
            return result;
        } else {
            std::cout << outcome.GetError().GetMessage().c_str() << std::endl;
            exit(1);
        }
    }
};