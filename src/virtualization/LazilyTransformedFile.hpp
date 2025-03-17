#ifndef LAZILY_TRANSFORMED_FILE_H
#define LAZILY_TRANSFORMED_FILE_H

#include <arrow/api.h>

#include <parquet/arrow/schema.h>
#include <parquet/statistics.h>
#include <parquet/metadata.h>
#include <parquet/column_writer.h>
#include <thread>

#include <btrblocks/arrow/ColumnStreamReader.hpp>

#include <cassert>
#include <chrono>
#include <avro/Specific.hh>

#include "../util/parallel.hpp"

#include "parquet/ColumnChunkWriter.hpp"
#include "VirtualizedFile.hpp"
#include "parquet/ParquetUtils.hpp"

class LazilyTransformedFile final : public VirtualizedFile {
    size_t requestCounter = 0;
    btrblocks::arrow::DirectoryReader directoryReader;
    const btrblocks::FileMetadata* metadata;
    std::unique_ptr<parquet::FileMetaData> parquetMetadata;
    std::string serializedParquetMetadata;
    static thread_local std::string buffer;
    static thread_local std::vector<uint8_t> curr_buffer;
    hpqp::enumerable_thread_specific<std::vector<btrblocks::arrow::ColumnStreamReader>> columnReaders;
    size_t size_;
    size_t metadata_offset;
    int combinedChunks_;
    std::mutex mtx;
    std::string path;
    std::vector<std::vector<int64_t>> uncompressedSizes;

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

    [[nodiscard]] static uint64_t getDictEncodedDataSize(uint64_t num_values, uint64_t num_unique_values) {
        const uint8_t byteLength = (std::bit_width(num_unique_values) + 7) / 8;
        return num_values * byteLength + 6 + ParquetUtils::GetVarintSize(num_values << 1) + ParquetUtils::GetVarintSize(((num_values + 7 )/ 8) << 1 | 1);
    }

    [[nodiscard]] static uint64_t getDataSize(const std::shared_ptr<arrow::Array>& arr, bool isDictionary) {
        uint64_t result;
        if (isDictionary || arr->type() == arrow::utf8()) {
            int64_t tuple_count = arr->data()->length;
            const auto* offsets = reinterpret_cast<const int32_t*>(arr->data()->buffers[1]->data());
            result = tuple_count * 4 + offsets[tuple_count];
        }else {
            result = arr->length() * arr->type()->byte_width();
        }
        if (isDictionary) return result;
        return result + 5 + ParquetUtils::GetVarintSize(arr->length() << 1);
    }

    [[nodiscard]] uint64_t getSize(int rowgroup, int column) const {
        const btrblocks::ColumnChunkInfo& chunk_metadata = getChunkInfo(rowgroup, column);
        uint64_t result = chunk_metadata.uncompressedSize;
        if (metadata->columns[column].type == btrblocks::ColumnType::STRING) result -= 4;
        return result + ParquetUtils::writePageWithoutData(getDataSize(rowgroup, column), chunk_metadata.tuple_count).size();
    }

    [[nodiscard]] static uint64_t getDictionarySize(uint64_t num_unique_values, uint64_t total_length) {
        const uint64_t dataSize = total_length + num_unique_values * sizeof(int32_t);
        return dataSize + ParquetUtils::writePageWithoutData(dataSize, num_unique_values, true).size();
    }

    static void writeChunk(int64_t chunkBegin, int64_t chunkEnd, S3InterfaceUtils::ByteRange byteRange, const std::shared_ptr<arrow::Array>& arr, size_t &offset, bool isDictionaryPage, size_t uncompressed_size, uint64_t unique_values = 0) {
        if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
            const int64_t begin = std::max(chunkBegin, byteRange.begin);
            const int64_t end = std::min(chunkEnd, byteRange.end);
            // should not be needed, but warns if a reader does something unexpected
            assert(begin == chunkBegin);
            assert(end == chunkEnd);

            curr_buffer.resize(uncompressed_size);

            if (arrow::is_dictionary(arr->type_id())) {
                const uint8_t byteLength = (std::bit_width(unique_values) + 7) / 8;
                ColumnChunkWriter::writeDictionaryEncodedChunk(
                    ParquetUtils::writePageWithoutData(getDictEncodedDataSize(arr->length(), unique_values),
                    arr->length(), false, true, byteLength),
                    std::static_pointer_cast<arrow::DictionaryArray>(arr)->indices(), curr_buffer, byteLength, 0);
            } else {
                ColumnChunkWriter::writeColumnChunk(
                ParquetUtils::writePageWithoutData(getDataSize(arr, isDictionaryPage),
                    arr->length(), isDictionaryPage, false),
                    arr, curr_buffer);
            }

            assert(begin - chunkBegin >= 0);
            assert(begin - chunkBegin < curr_buffer.size());
            assert(end + 1 - chunkBegin >= 0);
            assert(end + 1 - chunkBegin <= curr_buffer.size());
            assert(offset + end - begin + 1 <= buffer.size());
            memcpy(buffer.data() + offset, curr_buffer.data() + begin - chunkBegin, end - begin + 1);
            assert(curr_buffer.size() >= end - begin + 1);
            offset += end - begin + 1;
        }
    }

public:
    explicit LazilyTransformedFile(const std::string path, int combinedChunks = 16) :
            directoryReader(path), metadata(directoryReader.metadata()), combinedChunks_(combinedChunks), path(path),
            columnReaders([path, this]() {
                std::vector<btrblocks::arrow::ColumnStreamReader> localReaders;
                localReaders.reserve(metadata->num_columns);
                for (int i=0; i!=metadata->num_columns; i++) {
                   localReaders.emplace_back(path, metadata, i);
                }
                assert(localReaders.size() == metadata->num_columns);
                return localReaders;
            }){
        std::cout << "construct file" << std::endl;
        std::cout << "file has b" << metadata->num_chunks << " chunks" << std::endl;
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
        auto parquetWriterProperties = parquet::WriterProperties::Builder().build();
        parquet::arrow::ToParquetSchema(schema.get(), *parquetWriterProperties, *parquet::default_arrow_writer_properties(), &schemaDescriptor);
        auto builder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), parquetWriterProperties);
        uncompressedSizes = std::vector<std::vector<int64_t>>(metadata->num_columns, std::vector<int64_t>(metadata->num_chunks));

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
                uint64_t dictionary_count = 0;
                uint64_t dictionary_length = 0;
                auto& initial_info = getChunkInfo(i, j);
                uint64_t min = initial_info.min_value, max = initial_info.max_value;
                // collect statistics
                for (int chunk = 0; chunk != combinedChunks && i+chunk < metadata->num_chunks; chunk++){
                    const int chunkI = i + chunk;
                    auto& chunk_info = getChunkInfo(chunkI, j);
                    min = std::min(min, chunk_info.min_value);
                    max = std::max(max, chunk_info.max_value);
                    if (chunk_info.unique_tuple_count > 0 && chunk_info.unique_tuple_count < 1000) {
                        dictionary_count += chunk_info.unique_tuple_count;
                        dictionary_length += chunk_info.total_unique_length;
                    }
                }

                // predict chunk sizes
                for (int chunk = 0; chunk != combinedChunks && i+chunk < metadata->num_chunks; chunk++) {
                    const int chunkI = i + chunk;
                    auto& chunk_info = getChunkInfo(chunkI, j);
                    if (chunk_info.unique_tuple_count > 0 && chunk_info.unique_tuple_count < 1000) {
                        const uint64_t dataSize = getDictEncodedDataSize(chunk_info.tuple_count, dictionary_count);
                        uncompressedSizes[j][chunkI] = dataSize +
                            ParquetUtils::writePageWithoutData(dataSize, chunk_info.tuple_count, false, true).size()
                         - (6 + ParquetUtils::GetVarintSize(chunk_info.tuple_count << 1) + ParquetUtils::GetVarintSize(((chunk_info.tuple_count + 7 )/ 8) << 1 | 1));
                    }else {
                        uncompressedSizes[j][chunkI] = getSize(chunkI, j);
                    }
                    uncompressed_size += uncompressedSizes[j][chunkI];
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

                const bool isDictionary = dictionary_count != 0;
                const int64_t dictionary_page_offset = isDictionary ? total_bytes : -1;
                const int64_t dictionary_size = getDictionarySize(dictionary_count, dictionary_length);
                const int64_t data_page_offset = isDictionary ? total_bytes + dictionary_size : total_bytes;
                uncompressed_size += isDictionary ? dictionary_size : 0;
                columnChunk->Finish(tuple_count, dictionary_page_offset, -1, data_page_offset,
                    uncompressed_size, uncompressed_size, isDictionary, false,
                    {}, {});
                total_bytes += uncompressed_size;
                rowgroup_bytes += uncompressed_size;
            }
            rowGroupBuilder->Finish(rowgroup_bytes);
        }
        parquetMetadata = builder->Finish();
        serializedParquetMetadata = parquetMetadata->SerializeToString();
        size_ = total_bytes + serializedParquetMetadata.size() + 8;
        metadata_offset = size_ - 8 - serializedParquetMetadata.size();
    }

    size_t size() override {
        return size_;
    }

    std::shared_ptr<std::string> getRange(S3InterfaceUtils::ByteRange byteRange) override {
        std::cout << "range: " << byteRange.begin << " " << byteRange.end << std::endl;
        assert(byteRange.end <= size_);
        buffer.resize(byteRange.size());
        size_t offset = 0;
        if (byteRange.begin == 0) {
            buffer[0] = 'P';
            buffer[1] = 'A';
            buffer[2] = 'R';
            buffer[3] = '1';
            offset = 4;
        }
        auto& localReaders = columnReaders.local();
        std::shared_ptr<arrow::Array> arr;
        assert(localReaders.size() == metadata->num_columns);
        for (int i=0; i<metadata->num_chunks; i+=combinedChunks_) {
            int rowgroup = i / combinedChunks_;
            for (int j=0; j!=metadata->num_columns; j++) {
                auto columnChunkMeta = parquetMetadata->RowGroup(rowgroup)->ColumnChunk(j);
                int64_t chunkBegin = columnChunkMeta->has_dictionary_page() ?
                    columnChunkMeta->dictionary_page_offset() : columnChunkMeta->data_page_offset();

                arrow::ArrayVector arrays;
                arrow::StringBuilder builder;
                if (columnChunkMeta->has_dictionary_page()
                    && !(columnChunkMeta->dictionary_page_offset() > byteRange.end
                        || columnChunkMeta->data_page_offset() < byteRange.begin)) {

                    for (int chunk=0; chunk!=combinedChunks_ && i+chunk<metadata->num_chunks; chunk++) {
                        const int chunkI = i + chunk;
                        std::shared_ptr<arrow::Array> arr;
                        const auto status = localReaders[j].Read(chunkI, &arr);
                        assert(status.ok() && arr != nullptr);
                        arrays.push_back(arr);
                        if (arrow::is_dictionary(arr->type_id())) {
                            auto dictArray = std::static_pointer_cast<arrow::DictionaryArray>(arr)->dictionary();
                            auto status = builder.AppendArraySlice(*dictArray->data(), 0, dictArray->length());
                        }
                    }

                    std::shared_ptr<arrow::Array> dictionaryArr;
                    auto status = builder.Finish(&dictionaryArr);
                    const int64_t dictionaryPageSize = columnChunkMeta->data_page_offset() - columnChunkMeta->dictionary_page_offset();
                    writeChunk(chunkBegin, chunkBegin + dictionaryPageSize - 1, byteRange, dictionaryArr, offset, true, dictionaryPageSize);
                    chunkBegin += dictionaryPageSize;
                    for (int chunk=0; chunk!=combinedChunks_ && i+chunk<metadata->num_chunks; chunk++) {
                        const int chunkI = i + chunk;
                        int64_t chunkEnd = chunkBegin + uncompressedSizes[j][chunkI] - 1;
                        writeChunk(chunkBegin, chunkEnd, byteRange, arrays[chunkI - i], offset, false, uncompressedSizes[j][chunkI], dictionaryArr->length());
                        // Prepare next chunk
                        chunkBegin += uncompressedSizes[j][chunkI];
                    }
                }else {
                    for (int chunk=0; chunk!=combinedChunks_ && i+chunk<metadata->num_chunks; chunk++) {
                        const int chunkI = i + chunk;
                        int64_t chunkEnd = chunkBegin + uncompressedSizes[j][chunkI] - 1;

                        if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
                            auto status = localReaders[j].Read(chunkI, &arr);
                            writeChunk(chunkBegin, chunkEnd, byteRange, arr, offset, false, uncompressedSizes[j][chunkI]);
                        }
                        // Prepare next chunk
                        chunkBegin += uncompressedSizes[j][chunkI];
                    }
                }
            }
        }

        if (byteRange.end >= metadata_offset && byteRange.size() != 8) {
            auto begin = std::max(metadata_offset, static_cast<unsigned long>(byteRange.begin)) - metadata_offset;
            auto end = std::min(size_ - 8, static_cast<unsigned long>(byteRange.end + 1)) - metadata_offset;
            memcpy(buffer.data() + offset, serializedParquetMetadata.data(), serializedParquetMetadata.size());
            offset += serializedParquetMetadata.size();
        }
        // TODO allow partial footer requests
        if (byteRange.end == size_ - 1) {
            const int32_t s = serializedParquetMetadata.size();
            std::string footer = "xxxxPAR1";
            memcpy(footer.data(), &s, 4);
            memcpy(buffer.data() + offset, footer.data(), footer.size());
            offset += footer.size();
        }
        auto t2 = std::chrono::high_resolution_clock::now();
        std::shared_ptr<std::string> b(&buffer, [](std::string*){});
        return b;
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H