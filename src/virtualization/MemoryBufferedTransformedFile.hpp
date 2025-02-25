#ifndef MemoryBufferedTransformedFile_H
#define MemoryBufferedTransformedFile_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <arrow/DirectoryReader.hpp>

#include "VirtualizedFile.hpp"

class MemoryBufferedTransformedFile final : public VirtualizedFile {
    std::shared_ptr<arrow::Buffer> buffer;
public:
    explicit MemoryBufferedTransformedFile(const std::string& path){
        btrblocks::arrow::DirectoryReader directoryReader(path);
        std::shared_ptr<arrow::Table> table;
        if (!directoryReader.ReadTable(&table).ok()){
            std::cout << "error reading btr directory\n";
            exit(1);
        }

        const auto out = arrow::io::BufferOutputStream::Create().ValueOrDie();
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out));
        buffer = out->Finish().ValueOrDie();
    }


    size_t size() override {
        return buffer->size();
    }

    std::shared_ptr<std::string> getRange(S3InterfaceUtils::ByteRange byteRange) override {
        return std::make_shared<std::string>(reinterpret_cast<char*>(buffer->mutable_data()) + byteRange.begin, static_cast<size_t>(byteRange.size()));
    }
};

#endif