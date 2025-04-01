#pragma once
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>

#include <pqxx/pqxx>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "VirtualizedFile.hpp"

class PostgresBufferedFile final : public VirtualizedFile {
    static thread_local std::shared_ptr<std::string> result;
    static thread_local pqxx::connection conn;
    std::shared_ptr<arrow::Buffer> buffer;

    std::size_t size_;
public:

    static std::shared_ptr<arrow::Table> getCombinedTable(pqxx::work& tx) {
        // Prepare buffers to load data
        std::unordered_map<int, std::shared_ptr<arrow::Buffer>> rebuildFiles;
        for (const auto& [fileId, size] :
                 tx.query<int, int>("SELECT file_id, size FROM BufferedFiles WHERE finalized")){
            rebuildFiles[fileId] = arrow::AllocateBuffer(size).ValueOrDie();
        }

        // Populate buffers with data
        for (const auto& [fileId, fileOffset, size, content] :
                tx.query<int, int, int, std::basic_string<std::byte>>("Select file_id, file_offset, size, content FROM bufferedView")) {
            memcpy(rebuildFiles[fileId]->mutable_data() + fileOffset, content.data(), size);
        }

        // Read buffers into arrow
        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (const auto& singleBuffer : rebuildFiles){
            // Read parquet into arrow table
            const auto buffer_reader = std::make_shared<arrow::io::BufferReader>(singleBuffer.second);
            std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
            const auto status = parquet::arrow::OpenFile(buffer_reader, arrow::default_memory_pool(), &parquet_reader);
            if (!status.ok()) {
                std::cout << status << std::endl;
            }
            std::shared_ptr<arrow::Table> table;
            PARQUET_THROW_NOT_OK(parquet_reader->ReadTable(&table));
            tables.push_back(table);
        }
        if (tables.empty()) {
            return nullptr;
        }
        return arrow::ConcatenateTables(tables).ValueOrDie();
    }

    explicit PostgresBufferedFile(const std::string& p) {
        std::cout << "created postgres buffered file" << std::endl;
        pqxx::work tx(conn);


        auto combinedTable = getCombinedTable(tx);
        if (combinedTable == nullptr) {
            size_ = 0;
            return;
        }
        // Write table to buffer
        const auto out = arrow::io::BufferOutputStream::Create().ValueOrDie();
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*combinedTable, arrow::default_memory_pool(), out));
        buffer = out->Finish().ValueOrDie();
        std::cout << (*buffer)[buffer->size() - 4] << (*buffer)[buffer->size() - 3] << (*buffer)[buffer->size() - 2] << (*buffer)[buffer->size() - 1] << std::endl;
        std::cout << "size" << buffer->size() << " " << buffer->size() / 1024 << std::endl;
        size_ = buffer->size();
    }

    size_t size() override {
        return size_;
    }

    std::shared_ptr<std::string> getRange(const S3InterfaceUtils::ByteRange byteRange) override {
        result->resize(byteRange.size());
        memcpy(result->data(), buffer->data() + byteRange.begin, byteRange.size());
        return result;
    }
};