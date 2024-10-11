#ifndef LAZILY_TRANSFORMED_FILE_H
#define LAZILY_TRANSFORMED_FILE_H

#include <arrow/api.h>

#include <parquet/arrow/schema.h>
#include <parquet/statistics.h>
#include <parquet/metadata.h>
#include <arrow/DirectoryReader.hpp>

#include "VirtualizedFile.hpp"

class LazilyTransformedFile : public VirtualizedFile {
    size_t requestCounter = 0;
    btrblocks::arrow::DirectoryReader directoryReader;
    const btrblocks::FileMetadata* metadata;
    std::unique_ptr<parquet::FileMetaData> parquetMetadata;
    std::string serializedParquetMetadata;
    std::shared_ptr<arrow::Buffer> buffer;
    size_t size_;

    size_t calculateSizeFromFileMetadata(const btrblocks::FileMetadata* metadata){ return 1200000; }

    [[nodiscard]] uint64_t getSize(int rowgroup, int column) const {
        int partOffset = metadata->columns[column].part_offset;
        int currentChunk = 0;
        while (currentChunk + metadata->parts[partOffset].num_chunks < rowgroup) {
            currentChunk += metadata->parts[partOffset++].num_chunks;
        }
        int inline_chunk = rowgroup - currentChunk;
        return metadata->chunks[metadata->parts[partOffset].chunk_offset + inline_chunk].uncompressedSize;
    }
public:
    explicit LazilyTransformedFile(const std::string& path) :
            directoryReader(path), metadata(directoryReader.metadata()) {
        std::cout << metadata << std::endl;
        size_ = calculateSizeFromFileMetadata(metadata);
        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
        directoryReader.GetSchema(&schema);
        auto parquetWriterProperties = parquet::WriterProperties::Builder().build();
        parquet::arrow::ToParquetSchema(schema.get(), *parquetWriterProperties, *parquet::default_arrow_writer_properties(), &schemaDescriptor);
        auto builder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), parquetWriterProperties);

        int num_vals = 64000;
        for (int i=0; i!=metadata->num_chunks; i++) {
            int64_t total_bytes = 0;
            auto* rowGroupBuilder = builder->AppendRowGroup();
            rowGroupBuilder->set_num_rows(num_vals);
            for (int j=0; j!=metadata->num_columns; j++) {
                auto* columnChunk = rowGroupBuilder->NextColumnChunk();
                uint64_t uncompressed_size = getSize(i, j);
                parquet::EncodedStatistics statistics;
                statistics.set_max("");
                statistics.set_min("");
                statistics.set_distinct_count(0);
                statistics.set_null_count(0);
                columnChunk->SetStatistics(statistics);
                columnChunk->Finish(num_vals, 0, 0, 0,
                    uncompressed_size, uncompressed_size, false, false,
                    {}, {});
                total_bytes += uncompressed_size;
            }
            rowGroupBuilder->Finish(total_bytes);
        }
        parquetMetadata = builder->Finish();
        serializedParquetMetadata = parquetMetadata->SerializeToString();
    }

    size_t size() override {
        return size_;
    }

    std::string getRange(S3InterfaceUtils::ByteRange byteRange) override {
        std::vector<uint8_t> buffer(byteRange.size());
        requestCounter++;
        std::cout << byteRange.begin << " " << byteRange.end << " (" << byteRange.size() << ")\n";
        if (byteRange.begin == size_ - 8) {
            const int32_t s = serializedParquetMetadata.size();
            std::string result = "xxxxPAR1";
            memcpy(result.data(), &s, 4);
            return result;
        }
        if (byteRange.begin == size_ - 8 - serializedParquetMetadata.size()) {
            return parquetMetadata->SerializeToString();
        }
        int i = 0;
        while (parquetMetadata->RowGroup(i)->file_offset() < byteRange.begin) {
            i++;
        }
        std::cout << parquetMetadata->RowGroup(i)->file_offset() << std::endl;
        std::cout << "went through" << std::endl;
        return "";
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H