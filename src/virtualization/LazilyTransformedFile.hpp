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

inline std::mutex global_mtx;

class LazilyTransformedFile : public VirtualizedFile {
    size_t requestCounter = 0;
    btrblocks::arrow::DirectoryReader directoryReader;
    const btrblocks::FileMetadata* metadata;
    std::unique_ptr<parquet::FileMetaData> parquetMetadata;
    std::string serializedParquetMetadata;
    std::shared_ptr<arrow::Buffer> buffer;
    size_t size_;
    size_t metadata_offset;

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

    [[nodiscard]] const btrblocks::ColumnChunkInfo& getChunkInfo(const int rowgroup, const int column) const {
        int partOffset = metadata->columns[column].part_offset;
        int currentChunk = 0;
        while (currentChunk + metadata->parts[partOffset].num_chunks < rowgroup) {
            currentChunk += metadata->parts[partOffset++].num_chunks;
        }

        int inline_chunk = rowgroup - currentChunk;
        return metadata->chunks[metadata->parts[partOffset].chunk_offset + inline_chunk];
    }

    [[nodiscard]] uint64_t getSize(int rowgroup, int column) const {
        const btrblocks::ColumnChunkInfo& chunk_metadata = getChunkInfo(rowgroup, column);
        uint64_t result =  chunk_metadata.uncompressedSize;
        if (metadata->columns[column].type == btrblocks::ColumnType::STRING) result -= 4;
        std::cout << "size " << result << " "  << ParquetUtils::writePageWithoutData(result, chunk_metadata.tuple_count).size() << std::endl;
        return result + ParquetUtils::writePageWithoutData(result, chunk_metadata.tuple_count).size();
    }
public:
    explicit LazilyTransformedFile(const std::string& path) :
            directoryReader(path), metadata(directoryReader.metadata()) {

        for (int column_i=0; column_i!=metadata->num_columns; column_i++) {
            btrblocks::ColumnInfo& column_info = metadata->columns[column_i];
            int total_chunks = 0;
            for (int part_i=0; part_i!=column_info.num_parts; part_i++) {
                btrblocks::ColumnPartInfo& part_info = metadata->parts[column_info.part_offset + part_i];
                assert(part_info.num_chunks != 0);
                total_chunks += part_info.num_chunks;
            }
            std::cout << column_i << " has parts " << column_info.num_parts << std::endl;
            std::cout << column_i << " " << total_chunks << " is but expected" << metadata->num_chunks << std::endl;
            assert(total_chunks == metadata->num_chunks);
        }

        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
        directoryReader.GetSchema(&schema);
        auto parquetWriterProperties = parquet::WriterProperties::Builder().build();
        parquet::arrow::ToParquetSchema(schema.get(), *parquetWriterProperties, *parquet::default_arrow_writer_properties(), &schemaDescriptor);
        auto builder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), parquetWriterProperties);

        int64_t total_bytes = 4;
        for (int i=0; i!=metadata->num_chunks; i++) {
            int64_t rowgroup_bytes = 0;
            auto* rowGroupBuilder = builder->AppendRowGroup();
            uint64_t tuple_count = getChunkInfo(i, 0).tuple_count;
            rowGroupBuilder->set_num_rows(tuple_count);
            for (int j=0; j!=metadata->num_columns; j++) {
                auto* columnChunk = rowGroupBuilder->NextColumnChunk();
                uint64_t uncompressed_size = getSize(i, j);
                parquet::EncodedStatistics statistics;
                statistics.set_max("");
                statistics.set_min("");
                statistics.set_distinct_count(0);
                statistics.set_null_count(0);
                columnChunk->SetStatistics(statistics);
                columnChunk->Finish(tuple_count, -1, -1, total_bytes,
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
        metadata_offset = size_ - 8 - serializedParquetMetadata.size();
    }

    size_t size() override {
        return size_;
    }

    std::string getRange(S3InterfaceUtils::ByteRange byteRange) override {
        std::lock_guard guard(global_mtx);
        assert(metadata->parts[4].num_chunks == 21);
        std::vector<uint8_t> buffer(byteRange.size());
        requestCounter++;
        std::cout << "range " << byteRange.begin << " " << byteRange.end << " (" << byteRange.size() << ")\n";

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
                        std::cout << i << " " << j << std::endl;
                        assert(metadata->parts[4].num_chunks == 21 && 1);
                        directoryReader.GetRecordBatchReader({i}, {j}, &reader);
                        auto batch = reader->Next().ValueOrDie();
                        assert(metadata->parts[4].num_chunks == 21 && 11);
                        buffers[{i, j}] = ColumnChunkWriter::writeColumnChunk(
                            ParquetUtils::writePageWithoutData(uncompressed_size, num_values), batch->column(0), uncompressed_size);
                        assert(metadata->parts[4].num_chunks == 21 && 2);
                    }
                    auto& curr_buffer = buffers[{i, j}];
                    assert(curr_buffer.size() == chunkEnd - chunkBegin + 1);
                    assert(begin - chunkBegin >= 0);
                    assert(begin - chunkBegin < curr_buffer.size());
                    assert(end + 1 - chunkBegin >= 0);
                    assert(end + 1 - chunkBegin <= curr_buffer.size());
                    result.insert(result.end(), curr_buffer.begin() + begin - chunkBegin,
                        curr_buffer.begin() + end + 1 - chunkBegin);
                    assert(metadata->parts[4].num_chunks == 21 && 3);
                    std::cout << "yes" << (begin == chunkBegin) << (end == chunkEnd) << std::endl;
                }else {
                    std::cout << "no" << std::endl;
                }
            }
        }


        if (byteRange.end >= metadata_offset && byteRange.size() != 8) {
            std::cout << "footer" << std::endl;
            auto begin = std::max(metadata_offset, static_cast<unsigned long>(byteRange.begin)) - metadata_offset;
            auto end = std::min(size_ - 8, static_cast<unsigned long>(byteRange.end + 1)) - metadata_offset;
            result.insert(result.end(), serializedParquetMetadata.begin() + begin, serializedParquetMetadata.begin() + end);
        }
        // TODO allow partial footer requests
        if (byteRange.end == size_ - 1) {
            const int32_t s = serializedParquetMetadata.size();
            std::string footer = "xxxxPAR1";
            memcpy(footer.data(), &s, 4);
            result.insert(result.end(), footer.begin(), footer.end());
        }

        std::cout << result.size() << " vs " << byteRange.size() << std::endl;
        std::cout << byteRange.end << " of " << size_ << std::endl;
        assert(result.size() == byteRange.size());
        return std::string(result.begin(), result.end());
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H