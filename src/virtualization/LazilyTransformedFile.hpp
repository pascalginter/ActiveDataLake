#ifndef LAZILY_TRANSFORMED_FILE_H
#define LAZILY_TRANSFORMED_FILE_H

#include <arrow/api.h>

#include <parquet/arrow/schema.h>
#include <parquet/statistics.h>
#include <parquet/metadata.h>
#include <parquet/column_writer.h>
#include <arrow/DirectoryReader.hpp>

#include <cassert>
#include <avro/Specific.hh>

#include "parquet/ColumnChunkWriter.hpp"
#include "VirtualizedFile.hpp"
#include "parquet/ParquetUtils.hpp"

std::mutex global_mtx;

class LazilyTransformedFile : public VirtualizedFile {
    size_t requestCounter = 0;
    btrblocks::arrow::DirectoryReader directoryReader;
    const btrblocks::FileMetadata* metadata;
    std::unique_ptr<parquet::FileMetaData> parquetMetadata;
    std::string serializedParquetMetadata;
    std::shared_ptr<arrow::Buffer> buffer;
    size_t size_;

    std::map<std::pair<int, int>, std::vector<uint8_t>> buffers;

    [[nodiscard]] size_t calculateSizeFromFileMetadata(const btrblocks::FileMetadata* metadata) const {
        size_t result = 0;
        for (int i = 0; i != metadata->num_chunks; i++) {
            for (int j = 0; j != metadata->num_columns; j++) {
                result += getSize(i, j);
            }
        }
        return result;
    }

    [[nodiscard]] uint64_t getSize(int rowgroup, int column) const {
        int partOffset = metadata->columns[column].part_offset;
        std::cout << column << " eeee " << partOffset << std::endl;
        int currentChunk = 0;
        while (currentChunk + metadata->parts[partOffset].num_chunks < rowgroup) {
            currentChunk += metadata->parts[partOffset++].num_chunks;
        }
        std::cout << currentChunk << " aadaa " << partOffset << std::endl;
        int inline_chunk = rowgroup - currentChunk;
        uint64_t result =  metadata->chunks[metadata->parts[partOffset].chunk_offset + inline_chunk].uncompressedSize;
        std::cout << inline_chunk << " " << &metadata->chunks[metadata->parts[partOffset].chunk_offset + inline_chunk] << " " << rowgroup << ", " << column << ": " << result << std::endl;
        if (metadata->columns[column].type == btrblocks::ColumnType::STRING) result -= 4;
        return result + ParquetUtils::writePageWithoutData(result, 64000).size();
    }
public:
    explicit LazilyTransformedFile(const std::string& path) :
            directoryReader(path), metadata(directoryReader.metadata()) {
        std::cout << metadata << std::endl;
        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
        directoryReader.GetSchema(&schema);
        auto parquetWriterProperties = parquet::WriterProperties::Builder().build();
        parquet::arrow::ToParquetSchema(schema.get(), *parquetWriterProperties, *parquet::default_arrow_writer_properties(), &schemaDescriptor);
        auto builder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), parquetWriterProperties);

        int64_t total_bytes = 4;
        int num_vals = 64000;
        for (int i=0; i!=metadata->num_chunks; i++) {
            int64_t rowgroup_bytes = 0;
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
                columnChunk->Finish(num_vals, -1, -1, total_bytes,
                    uncompressed_size, uncompressed_size, false, false,
                    {{parquet::Encoding::PLAIN, 0}}, {});
                total_bytes += uncompressed_size;
                rowgroup_bytes += uncompressed_size;
            }
            rowGroupBuilder->Finish(rowgroup_bytes);
        }
        parquetMetadata = builder->Finish();
        serializedParquetMetadata = parquetMetadata->SerializeToString();
        size_ = calculateSizeFromFileMetadata(metadata) + serializedParquetMetadata.size() + 12;
    }

    size_t size() override {
        return size_;
    }

    std::string getRange(S3InterfaceUtils::ByteRange byteRange) override {
        std::vector<uint8_t> buffer(byteRange.size());
        requestCounter++;
        std::cout << "range " << byteRange.begin << " " << byteRange.end << " (" << byteRange.size() << ")\n";
        if (byteRange.begin == size_ - 8) {
            const int32_t s = serializedParquetMetadata.size();
            std::string result = "xxxxPAR1";
            memcpy(result.data(), &s, 4);
            return result;
        }
        if (byteRange.begin == size_ - 8 - serializedParquetMetadata.size()) {
            std::string result = parquetMetadata->SerializeToString();
            if (byteRange.end == size_ - 1) {
                const int32_t s = serializedParquetMetadata.size();
                result += "xxxxPAR1";
                memcpy(result.data() + s, &s, 4);
            }
            return result;
        }

        std::vector<uint8_t> result;
        for (int i=0; i!=metadata->num_chunks; i++) {
            for (int j=0; j!=metadata->num_columns; j++) {
                int64_t uncompressed_size = parquetMetadata->RowGroup(i)->ColumnChunk(j)->total_uncompressed_size();
                int64_t num_values = parquetMetadata->RowGroup(i)->ColumnChunk(j)->num_values();
                int64_t chunkBegin = parquetMetadata->RowGroup(i)->ColumnChunk(j)->data_page_offset();
                int64_t chunkEnd = chunkBegin + uncompressed_size - 1;
                if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
                    int64_t begin = std::max(chunkBegin, byteRange.begin);
                    int64_t end = std::min(chunkEnd, byteRange.end);
                    if (!buffers.contains({i, j})) {
                        std::shared_ptr<arrow::RecordBatchReader> reader;
                        directoryReader.GetRecordBatchReader({i}, {j}, &reader);
                        auto batch = reader->Next().ValueOrDie();
                        buffers[{i, j}] = ColumnChunkWriter::writeColumnChunk(
                            ParquetUtils::writePageWithoutData(uncompressed_size, num_values), batch->column(0));
                    }
                    auto& curr_buffer = buffers[{i, j}];
                    std::cout << curr_buffer.size() << " vs " << chunkEnd - chunkBegin + 1 << std::endl;
                    assert(curr_buffer.size() == chunkEnd - chunkBegin + 1);
                    assert(begin - chunkBegin >= 0);
                    assert(begin - chunkBegin < curr_buffer.size());
                    assert(end + 1 - chunkBegin >= 0);
                    assert(end + 1 - chunkBegin <= curr_buffer.size());
                    result.insert(result.end(), curr_buffer.begin() + begin - chunkBegin,
                        curr_buffer.begin() + end + 1 - chunkBegin);
                }
            }
        }
        std::cout << result.size() << " vs2 " << byteRange.size() << std::endl;
        return std::string(result.begin(), result.end());
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H