#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/s3-crt/S3CrtErrors.h>

#include "VirtualizedFile.hpp"

class PrefetchFile final : public VirtualizedFile {
    std::string key;
    std::vector<std::atomic<bool>> outstandingRequests;
    constexpr static std::string bucket = "adl-tpch";
    constexpr static size_t increment = 16 * (1 << 20);
    static std::shared_ptr<std::string> result;
    static thread_local Aws::S3Crt::S3CrtClient client;
public:
    explicit PrefetchFile(const std::string& p) : key(p + ".parquet"){
        std::cout << "created prefetch file" << std::endl;
    }

    size_t size() override {
        Aws::S3Crt::Model::HeadObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(key);
        const size_t size = client.HeadObject(request).GetResult().GetContentLength();
        result->resize(size);
        outstandingRequests = std::vector<std::atomic<bool>>((size + increment - 1) / increment + 1, false);
        for (size_t i = 0; i<size; i+= increment){
           Aws::S3Crt::Model::GetObjectRequest getRequest;
           getRequest.SetBucket(bucket);
           getRequest.SetKey(key);
           const size_t end = std::min(size-1, i+increment);
           const size_t s = end - i + 1;
           getRequest.SetRange(S3InterfaceUtils::ByteRange(i, end).toRangeString());
           client.GetObjectAsync(getRequest, [this, i, s](
                   const Aws::S3Crt::S3CrtClient*,
                   const Aws::S3Crt::Model::GetObjectRequest&,
                   Aws::S3Crt::Model::GetObjectOutcome outcome,
                   const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
               outcome.GetResult().GetBody().read(result->data() + i, s);
               outstandingRequests[i / increment] = true;
           });
        }
        return size;
    }

    std::shared_ptr<std::string> getRange(S3InterfaceUtils::ByteRange byteRange) override {
        size_t begin = byteRange.begin / increment, end = (byteRange.end + increment - 1) / increment;
        for (size_t i=begin; i!=end; i++) outstandingRequests[i].wait(true);
        auto res = std::make_shared<std::string>("");
        res->resize(byteRange.size());
        memcpy(res->data(), result->data() + byteRange.begin, byteRange.size());
        return res;
    }
};