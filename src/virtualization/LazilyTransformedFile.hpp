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
#include <btrblocks/arrow/DirectoryReader.hpp>
#include <oatpp/data/stream/Stream.hpp>

#include "../util/parallel.hpp"

#include "parquet/ColumnChunkWriter.hpp"
#include "VirtualizedFile.hpp"
#include "parquet/ParquetUtils.hpp"

struct LazilyTransformedFileState {
    int combinedChunks;
    const btrblocks::FileMetadata* metadata;
    std::unique_ptr<parquet::FileMetaData> parquetMetadata;
    std::string serializedParquetMetadata;
    std::vector<std::vector<int64_t>> uncompressedSizes;
    size_t size;
    size_t metadata_offset;

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
};

class LazyReadCallback : public oatpp::data::stream::ReadCallback {
    LazilyTransformedFileState& state;
    std::shared_ptr<std::vector<btrblocks::arrow::ColumnStreamReader>> localReaders;
    S3InterfaceUtils::ByteRange byteRange;

    int i=0;
    int j=0;
    int chunk=0;
    bool finishedPrevChunk = true;
    size_t tupleOffset = 0;
    std::shared_ptr<arrow::Array> arr = nullptr;

    ColumnChunkWriter::WriteResult writeChunk(void* buffer, size_t maxSize, size_t valueOffset, int64_t chunkBegin, int64_t chunkEnd, S3InterfaceUtils::ByteRange byteRange, const std::shared_ptr<arrow::Array>& arr, bool isDictionaryPage, uint64_t unique_values = 0) {
        if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
            const int64_t begin = std::max(chunkBegin, byteRange.begin);
            const int64_t end = std::min(chunkEnd, byteRange.end);
            // should not be needed, but warns if a reader does something unexpected
            assert(begin == chunkBegin);
            assert(end == chunkEnd);

            if (arrow::is_dictionary(arr->type_id())) {
                const uint8_t byteLength = (std::bit_width(unique_values) + 7) / 8;
                return ColumnChunkWriter::writeDictionaryEncodedChunk(buffer, maxSize, valueOffset,
                    ParquetUtils::writePageWithoutData(state.getDictEncodedDataSize(arr->length(), unique_values),
                    arr->length(), false, true, byteLength),
                    std::static_pointer_cast<arrow::DictionaryArray>(arr)->indices(), byteLength);
            } else {
                return ColumnChunkWriter::writeColumnChunk(buffer, maxSize, valueOffset,
                ParquetUtils::writePageWithoutData(state.getDataSize(arr, isDictionaryPage),
                    arr->length(), isDictionaryPage, false),
                    arr);
            }
        }
    }
public:
    LazyReadCallback(LazilyTransformedFileState& state,
        std::shared_ptr<std::vector<btrblocks::arrow::ColumnStreamReader>> readers,
        S3InterfaceUtils::ByteRange byteRange)
      : state(state), localReaders(readers), byteRange(byteRange) {
        std::cout << "meta " << state.metadata << std::endl;
    }


    oatpp::v_io_size read(void* buffer, oatpp::v_io_size count, oatpp::async::Action& action) override {
        std::cout << "max (" << byteRange.begin << ", " << byteRange.end << ") "<< count << std::endl;
        if (byteRange.begin == 0) {
            static_cast<char*>(buffer)[0] = 'P';
            static_cast<char*>(buffer)[1] = 'A';
            static_cast<char*>(buffer)[2] = 'R';
            static_cast<char*>(buffer)[3] = '1';
            byteRange.begin = 4;
            std::cout << 4 << std::endl;
            return 4;
        }

        for ( ; i<state.metadata->num_chunks; i+=state.combinedChunks) {
            int rowgroup = i / state.combinedChunks;
            for ( ; j!=state.metadata->num_columns; j++) {
                auto columnChunkMeta = state.parquetMetadata->RowGroup(rowgroup)->ColumnChunk(j);
                int64_t chunkBegin = columnChunkMeta->has_dictionary_page() ?
                    columnChunkMeta->dictionary_page_offset() : columnChunkMeta->data_page_offset();

                if (columnChunkMeta->has_dictionary_page()
                    && !(columnChunkMeta->dictionary_page_offset() > byteRange.end
                        || columnChunkMeta->data_page_offset() < byteRange.begin)) {
                    std::cout << "Oh no !!!!!!!" << std::endl;
                    arrow::ArrayVector arrays;
                    arrow::StringBuilder builder;
                    for (int chunk=0; chunk!=state.combinedChunks && i+chunk<state.metadata->num_chunks; chunk++) {
                        const int chunkI = i + chunk;
                        std::shared_ptr<arrow::Array> arr;
                        auto status = (*localReaders)[j].Read(chunkI, &arr);
                        assert(status.ok() && arr != nullptr);
                        arrays.push_back(arr);
                        if (arrow::is_dictionary(arr->type_id())) {
                            auto dictArray = std::static_pointer_cast<arrow::DictionaryArray>(arr)->dictionary();
                            status = builder.AppendArraySlice(*dictArray->data(), 0, dictArray->length());
                        }
                    }

                    std::shared_ptr<arrow::Array> dictionaryArr;
                    auto status = builder.Finish(&dictionaryArr);
                    const int64_t dictionaryPageSize = columnChunkMeta->data_page_offset() - columnChunkMeta->dictionary_page_offset();
                    writeChunk(buffer, count, 0, chunkBegin, chunkBegin + dictionaryPageSize - 1, byteRange, dictionaryArr, true, dictionaryPageSize);
                    chunkBegin += dictionaryPageSize;
                    for (int chunk=0; chunk!=state.combinedChunks && i+chunk<state.metadata->num_chunks; chunk++) {
                        const int chunkI = i + chunk;
                        int64_t chunkEnd = chunkBegin + state.uncompressedSizes[j][chunkI] - 1;
                        writeChunk(buffer, count, 0, chunkBegin, chunkEnd, byteRange, arrays[chunkI - i], false, dictionaryArr->length());
                        // Prepare next chunk
                        chunkBegin += state.uncompressedSizes[j][chunkI];
                    }
                }else {
                    for (; chunk!=state.combinedChunks && i+chunk<state.metadata->num_chunks; chunk++) {
                        const int chunkI = i + chunk;
                        int64_t chunkEnd = chunkBegin + state.uncompressedSizes[j][chunkI] - 1;

                        if (!(chunkEnd < byteRange.begin || chunkBegin > byteRange.end)) {
                            std::cout << byteRange.begin << " " << chunkBegin << " "  << chunkEnd << " " << byteRange.end << std::endl;
                            if (tupleOffset == 0) {
                                auto status = (*localReaders)[j].Read(chunkI, &arr);
                            }
                            auto writeResult = writeChunk(buffer, count, tupleOffset, chunkBegin, chunkEnd, byteRange, arr, false, state.uncompressedSizes[j][chunkI]);
                            tupleOffset = writeResult.tuples;
                            if (tupleOffset == arr->length()) {
                                tupleOffset = 0;
                                chunk = (chunk + 1) % state.combinedChunks;
                                byteRange.begin = chunkEnd + 1;
                            }
                            std::cout << "from chunk " << writeResult.bytes << std::endl;
                            return writeResult.bytes;
                        }
                        // Prepare next chunk
                        chunkBegin += state.uncompressedSizes[j][chunkI];
                    }
                }
            }
        }
        if (byteRange.end >= state.metadata_offset && byteRange.size() > 8) {
            auto begin = std::max(state.metadata_offset, static_cast<unsigned long>(byteRange.begin)) - state.metadata_offset;
            auto end = std::min(state.size - 8, static_cast<unsigned long>(byteRange.end + 1)) - state.metadata_offset;
            auto n = std::min(count, static_cast<int64_t>(state.serializedParquetMetadata.size() - tupleOffset));
            memcpy(buffer, state.serializedParquetMetadata.data() + tupleOffset, n);
            tupleOffset += n;
            if (n == 0) tupleOffset = 0;
            std::cout << "from metadata " << n << std::endl;
            return n;
        }
        // TODO allow partial footer requests
        if (byteRange.end == state.size - 1 && byteRange.begin < byteRange.end) {
            const int32_t s = state.serializedParquetMetadata.size();
            std::string footer = "xxxxPAR1";
            memcpy(footer.data(), &s, 4);
            memcpy(buffer, footer.data(), footer.size());
            std::cout << "from footer 8" << std::endl;
            byteRange.begin = state.size;
            return 8;
        }
        std::cout << "done with request" << std::endl;
        return 0;
    }
};

class LazilyTransformedFile final : public VirtualizedFile {
    LazilyTransformedFileState state;

    btrblocks::arrow::DirectoryReader directoryReader;
    hpqp::enumerable_thread_specific<std::shared_ptr<std::vector<btrblocks::arrow::ColumnStreamReader>>> columnReaders;
    std::string path;

    [[nodiscard]] size_t calculateSizeFromFileMetadata(const btrblocks::FileMetadata* metadata) const {
        size_t result = 0;
        for (int i = 0; i != metadata->num_chunks; i++) {
            for (int j = 0; j != metadata->num_columns; j++) {
                result += state.getSize(i, j);
            }
        }
        return result;
    }

public:
    explicit LazilyTransformedFile(const std::string path, int combinedChunks = 1) :
            state(), directoryReader(path), path(path),
            columnReaders([path, this]() {
                auto localReaders = std::make_shared<std::vector<btrblocks::arrow::ColumnStreamReader>>();
                const auto* metadata = directoryReader.metadata();
                localReaders->reserve(metadata->num_columns);
                for (int i=0; i!=metadata->num_columns; i++) {
                   localReaders->emplace_back(path, metadata, i);
                }
                assert(localReaders->size() == metadata->num_columns);
                return localReaders;
            }){
        state.metadata = directoryReader.metadata();
        state.combinedChunks = combinedChunks;

        for (int column_i=0; column_i!=state.metadata->num_columns; column_i++) {
            btrblocks::ColumnInfo& column_info = state.metadata->columns[column_i];
            int total_chunks = 0;
            for (int part_i=0; part_i!=column_info.num_parts; part_i++) {
                btrblocks::ColumnPartInfo& part_info = state.metadata->parts[column_info.part_offset + part_i];
                assert(part_info.num_chunks != 0);
                total_chunks += part_info.num_chunks;
            }
            assert(total_chunks == state.metadata->num_chunks);
        }

        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<parquet::SchemaDescriptor> schemaDescriptor;
        auto status = directoryReader.GetSchema(&schema);
        const auto parquetWriterProperties = parquet::WriterProperties::Builder().build();
        status = parquet::arrow::ToParquetSchema(schema.get(), *parquetWriterProperties, *parquet::default_arrow_writer_properties(), &schemaDescriptor);
        auto builder = parquet::FileMetaDataBuilder::Make(schemaDescriptor.get(), parquetWriterProperties);
        state.uncompressedSizes = std::vector<std::vector<int64_t>>(state.metadata->num_columns, std::vector<int64_t>(state.metadata->num_chunks));

        int64_t total_bytes = 4;
        for (int i=0; i<state.metadata->num_chunks; i+=combinedChunks) {
            int64_t rowgroup_bytes = 0;
            auto* rowGroupBuilder = builder->AppendRowGroup();
            uint64_t tuple_count = 0;
            for (int chunk=0; chunk!=combinedChunks && i+chunk<state.metadata->num_chunks; chunk++) {
                tuple_count += state.getChunkInfo(i+chunk, 0).tuple_count;
            }
            rowGroupBuilder->set_num_rows(tuple_count);
            for (int j=0; j!=state.metadata->num_columns; j++) {
                btrblocks::ColumnInfo& column_info = state.metadata->columns[j];
                auto* columnChunk = rowGroupBuilder->NextColumnChunk();
                uint64_t uncompressed_size = 0;
                uint64_t dictionary_count = 0;
                uint64_t dictionary_length = 0;
                auto& initial_info = state.getChunkInfo(i, j);
                uint64_t min = initial_info.min_value, max = initial_info.max_value;
                // collect statistics
                for (int chunk = 0; chunk != combinedChunks && i+chunk < state.metadata->num_chunks; chunk++){
                    const int chunkI = i + chunk;
                    auto& chunk_info = state.getChunkInfo(chunkI, j);
                    min = std::min(min, chunk_info.min_value);
                    max = std::max(max, chunk_info.max_value);
                    if (chunk_info.unique_tuple_count > 0 && chunk_info.unique_tuple_count < 1000) {
                        dictionary_count += chunk_info.unique_tuple_count;
                        dictionary_length += chunk_info.total_unique_length;
                    }
                }

                // predict chunk sizes
                for (int chunk = 0; chunk != combinedChunks && i+chunk < state.metadata->num_chunks; chunk++) {
                    const int chunkI = i + chunk;
                    auto& chunk_info = state.getChunkInfo(chunkI, j);
                    if (chunk_info.unique_tuple_count > 0 && chunk_info.unique_tuple_count < 1000) {
                        const uint64_t dataSize = state.getDictEncodedDataSize(chunk_info.tuple_count, dictionary_count);
                        state.uncompressedSizes[j][chunkI] = dataSize +
                            ParquetUtils::writePageWithoutData(dataSize, chunk_info.tuple_count, false, true).size()
                         - (6 + ParquetUtils::GetVarintSize(chunk_info.tuple_count << 1) + ParquetUtils::GetVarintSize(((chunk_info.tuple_count + 7 )/ 8) << 1 | 1));
                    }else {
                        state.uncompressedSizes[j][chunkI] = state.getSize(chunkI, j);
                    }
                    uncompressed_size += state.uncompressedSizes[j][chunkI];
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
                const int64_t dictionary_size = state.getDictionarySize(dictionary_count, dictionary_length);
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
        state.parquetMetadata = builder->Finish();
        state.serializedParquetMetadata = state.parquetMetadata->SerializeToString();
        state.size = total_bytes + state.serializedParquetMetadata.size() + 8;
        state.metadata_offset = state.size - 8 - state.serializedParquetMetadata.size();
    }

    size_t size() override {
        return state.size;
    }

    std::shared_ptr<oatpp::data::stream::ReadCallback> getRange(S3InterfaceUtils::ByteRange byteRange) override {
        std::cout << "range: " << byteRange.begin << " " << byteRange.end << std::endl;
        assert(byteRange.end <= state.size);
        return std::make_shared<LazyReadCallback>(state, columnReaders.local(), byteRange);
    }
};

#endif // LAZILY_TRANSFORMED_FILE_H