#include <fstream>
#include <filesystem>
#include <vector>
#include "VirtualizedFile.hpp"

thread_local std::ifstream file;

class LocalFile : public VirtualizedFile {
    std::filesystem::path path;
public:
    explicit LocalFile(const std::string& p) : path(p){
        std::cout << "created local file" << std::endl;
    }

    size_t size() override {
        return std::filesystem::file_size(path);
    }

    std::string getRange(S3InterfaceUtils::ByteRange byteRange) override {
        if (!file.is_open()) file.open(path);
        file.seekg(byteRange.begin);
        std::vector<char> buffer(byteRange.size());
        file.read(buffer.data(), byteRange.size());
        return {buffer.data(), buffer.size()};
    }
};