#ifndef LAZILY_TRANSFORMED_FILE_H
#define LAZILY_TRANSFORMED_FILE_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <arrow/DirectoryReader.hpp>

#include "VirtualizedFile.hpp"

class LazilyTransformedFile : public VirtualizedFile {
    std::shared_ptr<arrow::Buffer> buffer;
    size_t size_;
public:
    explicit LazilyTransformedFile(const std::string& path){
        /*btrblocks::arrow::DirectoryReader directoryReader(path);
        directoryReader.
        std::shared_ptr<arrow::Table> table;
        if (!directoryReader.ReadTable(&table).ok()){
            std::cout << "error reading btr directory\n";
            exit(1);
        }

        auto out = arrow::io::BufferOutputStream::Create().ValueOrDie();
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out));
        buffer = out->Finish().ValueOrDie();*/
    }


    size_t size() override {
        return size_;
    }

    std::string getRange(S3InterfaceUtils::ByteRange byteRange) override {
        return std::string{reinterpret_cast<char*>(buffer->mutable_data()) + byteRange.begin, byteRange.size()};
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H