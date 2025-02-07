#ifndef LAZILY_TRANSFORMED_FILE_H
#define LAZILY_TRANSFORMED_FILE_H

#include <arrow/api.h>

#include <parquet/arrow/schema.h>
#include <parquet/statistics.h>
#include <parquet/metadata.h>
#include <parquet/column_writer.h>

#include <btrblocks/arrow/ColumnStreamReader.hpp>

#include <cassert>
#include <avro/Specific.hh>

#include "parquet/ColumnChunkWriter.hpp"
#include "VirtualizedFile.hpp"
#include "parquet/ParquetUtils.hpp"

class LazilyTransformedFile : public VirtualizedFile {
    size_t requestCounter = 0;
    btrblocks::arrow::DirectoryReader directoryReader;
    const btrblocks::FileMetadata* metadata;
    std::unique_ptr<parquet::FileMetaData> parquetMetadata;
    std::string serializedParquetMetadata;
    std::shared_ptr<arrow::Buffer> buffer;
    size_t size_;
    size_t metadata_offset;
    int combinedChunks_;
    std::mutex mtx;
    std::string path;

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

    // TODO deduplicate logic
    [[nodiscard]] uint64_t getDataSize(int rowgroup, int column) const {
        const btrblocks::ColumnChunkInfo& chunk_metadata = getChunkInfo(rowgroup, column);
        uint64_t result =  chunk_metadata.uncompressedSize;
        if (metadata->columns[column].type == btrblocks::ColumnType::STRING) result -= 4;
        return result + 5 + ParquetUtils::GetVarintSize(chunk_metadata.tuple_count << 1);
    }

    [[nodiscard]] uint64_t getSize(int rowgroup, int column) const {
        const btrblocks::ColumnChunkInfo& chunk_metadata = getChunkInfo(rowgroup, column);
        uint64_t result =  chunk_metadata.uncompressedSize;
        if (metadata->columns[column].type == btrblocks::ColumnType::STRING) result -= 4;
        return result + ParquetUtils::writePageWithoutData(result, chunk_metadata.tuple_count).size();
    }


public:
    explicit LazilyTransformedFile(const std::string& path, int combinedChunks = 64) :
            directoryReader(path), metadata(directoryReader.metadata()), combinedChunks_(combinedChunks), path(path) {

        for (int column_i=0; column_i!=metadata->num_columns; column_i++) {
            btrblocks::ColumnInfo& column_info = metadata->columns[column_i];
            int total_chunks = 0;
            for (int part_i=0; part_i!=column_info.num_parts; part_i++) {
                btrblocks::ColumnPartInfo& part_info = metadata->parts[column_info.part_offset + part_i];
                assert(part_info.num_chunks != 0);
                total_chunks += part_info.num_chunks;
            }
            assert(total_chunks == metadata->num_chunks);
        }

        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
        directoryReader.GetSchema(&schema);
        std::cout << schema->ToString() << std::endl;
        std::cout << directoryReader.metadata() << std::endl;
        std::cout << metadata << std::endl;
        auto parquetWriterProperties = parquet::WriterProperties::Builder().build();
        parquet::arrow::ToParquetSchema(schema.get(), *parquetWriterProperties, *parquet::default_arrow_writer_properties(), &schemaDescriptor);
        auto builder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), parquetWriterProperties);

        int64_t total_bytes = 4;
        for (int i=0; i<metadata->num_chunks; i+=combinedChunks) {
            int64_t rowgroup_bytes = 0;
            auto* rowGroupBuilder = builder->AppendRowGroup();
            uint64_t tuple_count = 0;
            for (int chunk=0; chunk!=combinedChunks && i+chunk<metadata->num_chunks; chunk++) {
                tuple_count += getChunkInfo(i+chunk, 0).tuple_count;
            }
            rowGroupBuilder->set_num_rows(tuple_count);
            for (int j=0; j!=metadata->num_columns; j++) {
                btrblocks::ColumnInfo& column_info = metadata->columns[j];
                auto* columnChunk = rowGroupBuilder->NextColumnChunk();
                uint64_t uncompressed_size = 0;
                auto& initial_info = getChunkInfo(i, j);
                uint64_t min = initial_info.min_value, max = initial_info.max_value;
                for (int chunk = 0; chunk != combinedChunks && i+chunk < metadata->num_chunks; chunk++){
                    auto& chunk_info = getChunkInfo(i+chunk, j);
                    min = std::min(min, chunk_info.min_value);
                    max = std::max(max, chunk_info.max_value);
                    uncompressed_size += getSize(i+chunk, j);
                }

                parquet::EncodedStatistics statistics;
                size_t num_bytes;
                switch (column_info.type) {
                    case btrblocks::ColumnType::INTEGER:
                        num_bytes = 4;
                        break;
                    case btrblocks::ColumnType::STRING:
                        num_bytes = 8;
                        break;
                    default:
                        assert(false);
                }
                statistics.set_min(std::string(reinterpret_cast<char*>(&min), num_bytes));
                statistics.set_max(std::string(reinterpret_cast<char*>(&max), num_bytes));
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
        assert(byteRange.end <= size_);
        requestCounter++;

        std::vector<uint8_t> result;
        result.reserve(byteRange.size());
        result.assign(byteRange.size(), 0);
        size_t offset = 0;
        if (byteRange.begin == 0) {
            result[0] = 'P';
            result[1] = 'A';
            result[2] = 'R';
            result[3] = '1';
            offset = 4;
        }

        std::shared_ptr<arrow::Array> arr;
        std::vector<btrblocks::arrow::ColumnStreamReader> columnReaders;
        columnReaders.reserve(metadata->num_columns);
        for (int i=0; i!=metadata->num_columns; i++) {
            columnReaders.emplace_back(path, metadata, i);
        }
        for (int i=0; i<metadata->num_chunks; i+=combinedChunks_) {
            int rowgroup = i / combinedChunks_;
            for (int j=0; j!=metadata->num_columns; j++) {
                int64_t chunkBegin = parquetMetadata->RowGroup(rowgroup)->ColumnChunk(j)->data_page_offset();
                for (int chunk=0; chunk!=combinedChunks_ && i+chunk<metadata->num_chunks; chunk++) {
                    int chunkI = i + chunk;
                    int64_t uncompressed_size = getSize(chunkI, j);
                    int64_t num_values = getChunkInfo(chunkI, j).tuple_count;
                    int64_t chunkEnd = chunkBegin + uncompressed_size - 1;
                    if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
                        int64_t begin = std::max(chunkBegin, byteRange.begin);
                        int64_t end = std::min(chunkEnd, byteRange.end);

                        auto status = columnReaders[j].Read(chunkI, &arr);
                        assert(status.ok());
                        auto curr_buffer = ColumnChunkWriter::writeColumnChunk(
                            ParquetUtils::writePageWithoutData(
                                getDataSize(chunkI, j), num_values), arr, uncompressed_size);
                        assert(curr_buffer.size() == chunkEnd - chunkBegin + 1);
                        assert(begin - chunkBegin >= 0);
                        assert(begin - chunkBegin < curr_buffer.size());
                        assert(end + 1 - chunkBegin >= 0);
                        assert(end + 1 - chunkBegin <= curr_buffer.size());
                        memcpy(result.data() + offset, curr_buffer.data() + begin - chunkBegin, end - begin + 1);
                        offset += end - begin + 1;
                    }
                    // Prepare next chunk
                    chunkBegin += uncompressed_size;
                }
            }
        }

        if (byteRange.end >= metadata_offset && byteRange.size() != 8) {
            auto begin = std::max(metadata_offset, static_cast<unsigned long>(byteRange.begin)) - metadata_offset;
            auto end = std::min(size_ - 8, static_cast<unsigned long>(byteRange.end + 1)) - metadata_offset;
            memcpy(result.data() + offset, serializedParquetMetadata.data(), serializedParquetMetadata.size());
            offset += serializedParquetMetadata.size();
        }
        // TODO allow partial footer requests
        if (byteRange.end == size_ - 1) {
            const int32_t s = serializedParquetMetadata.size();
            std::string footer = "xxxxPAR1";
            memcpy(footer.data(), &s, 4);
            memcpy(result.data() + offset, footer.data(), footer.size());
        }
        std::cout << byteRange.begin << " " <<  byteRange.end << " of " << size_ << std::endl;
        std::cout << result.size() << " " << byteRange.size() << std::endl;
        assert(result.size() == byteRange.size());
        return {result.begin(), result.end()};
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H