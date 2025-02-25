#include <fstream>
#include <filesystem>
#include <vector>
#include "VirtualizedFile.hpp"

inline thread_local std::ifstream file;

class LocalFile final : public VirtualizedFile {
    std::filesystem::path path;
    static thread_local std::shared_ptr<std::string> result;
public:
    explicit LocalFile(const std::string& p) : path(p){
        std::cout << "created local file" << std::endl;
    }

    size_t size() override {
        return std::filesystem::file_size(path);
    }

    std::shared_ptr<std::string> getRange(S3InterfaceUtils::ByteRange byteRange) override {
        result->resize(byteRange.size());
        if (!file.is_open()) file.open(path);
        file.seekg(byteRange.begin);
        file.read(result->data(), byteRange.size());
        return result;
    }
};