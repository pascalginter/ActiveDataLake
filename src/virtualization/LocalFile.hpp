#pragma once
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>

#include "VirtualizedFile.hpp"

inline thread_local std::ifstream file;

class MemoryMappedFile{
    int file;
    void* data;

public:
    size_t size;
    explicit MemoryMappedFile(const char* fileName){

        file = open(fileName, O_RDONLY);
        if (file < 0){
            std::cout << "file < 0" << std::endl;
            exit(2);
        }
        size = lseek(file, 0, SEEK_END);
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, file, 0);
        madvise(data, size, MADV_SEQUENTIAL);
        madvise(data, size, MADV_WILLNEED);
        if (data == MAP_FAILED){
            std::cout << "MAP_FAILED" << std::endl;
            exit(3);
        }
    }
    ~MemoryMappedFile(){
        munmap(data, size);
        close(file);
    }

    [[nodiscard]] const char* begin() const{
        return static_cast<char*>(data);
    }

    [[nodiscard]] const char* end() const {
        return  static_cast<char*>(data) + size;
    }
};

class MMapRangeReadCallback : public oatpp::data::stream::ReadCallback {
    const char* data;
    size_t total;
    size_t pos = 0;

public:
    MMapRangeReadCallback(const char* ptr, size_t size)
      : data(ptr), total(size) {}

    oatpp::v_io_size read(void* buffer, oatpp::v_io_size count, oatpp::async::Action& action) override {
        size_t toRead = std::min(count, (oatpp::v_io_size)(total - pos));
        if (toRead == 0) return 0; // done
        memcpy(buffer, data + pos, toRead);
        pos += toRead;
        return toRead;
    }
};

class LocalFile final : public VirtualizedFile {
    static thread_local std::shared_ptr<std::string> result;
    MemoryMappedFile file;
public:
    explicit LocalFile(const std::string& p) : file(p.c_str()) {
        std::cout << "created local file" << std::endl;
    }

    size_t size() override {
        return file.size;
    }

    std::shared_ptr<oatpp::data::stream::ReadCallback> getRange(S3InterfaceUtils::ByteRange byteRange) override{
        auto callback = std::make_shared<MMapRangeReadCallback>(
            file.begin() + byteRange.begin,
            byteRange.size()
        );

        return callback;
    }
};