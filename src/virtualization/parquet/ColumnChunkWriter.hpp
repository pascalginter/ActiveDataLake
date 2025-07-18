#pragma once

class ColumnChunkWriter {
public:
    struct WriteResult {
        size_t bytes;
        size_t tuples;
    };
private:
    template <typename T>
    static WriteResult writeNumericColumnChunk(void* buffer,
                                               const size_t maxSize,
                                               const size_t tupleOffset,
                                               const std::vector<uint8_t>& header,
                                               const std::shared_ptr<arrow::Array>& array) {
        size_t offset = 0;
        if (tupleOffset == 0) {
            memcpy(buffer, header.data(), header.size());
            offset = header.size();
        }
        size_t n = std::min(array->length() - tupleOffset, (maxSize - offset) / sizeof(T));
        memcpy(buffer + offset, array->data()->buffers[1]->data() + tupleOffset * sizeof(T), n * sizeof(T));
        return {offset + n * sizeof(T), tupleOffset + n};
    }

    static WriteResult writeStringColumnChunk(void* buffer,
                                       const size_t maxSize,
                                       const size_t tupleOffset,
                                       const std::vector<uint8_t>& header,
                                       const std::shared_ptr<arrow::Array>& array) {
        const uint8_t* src = array->data()->buffers[2]->data();
        auto* offsets = reinterpret_cast<const int32_t*>(array->data()->buffers[1]->data());
        size_t curr_offset = 0;
        if (tupleOffset == 0) {
            memcpy(buffer, header.data(), header.size());
            curr_offset = header.size();
        }

        const int32_t n = array->length();
        size_t i=tupleOffset;
        for (; i!=n && curr_offset < maxSize; i++) {
            int32_t length = offsets[i+1] - offsets[i];
            memcpy(buffer + curr_offset, &length, sizeof(int32_t));
            curr_offset += 4;
            memcpy(buffer + curr_offset, src + offsets[i], length);
            curr_offset += length;
        }
        return {curr_offset, i};
    }
public:
    static WriteResult writeDictionaryEncodedChunk(void* buffer,
                                            const int64_t maxSize,
                                            const size_t tupleOffset,
                                            const std::vector<uint8_t>& header,
                                            const std::shared_ptr<arrow::Array>& array,
                                            const uint8_t byteLength) {
        assert(byteLength == 1);
        const uint8_t* data = array->data()->buffers[1]->data();
        size_t offset = 0;
        if (tupleOffset == 0) {
            offset = header.size();
            assert(offset <= maxSize);
            memcpy(buffer, header.data(), header.size());
        }

        const uint64_t n = std::min(array->length(), maxSize);
        for (int i=tupleOffset; i!=n ; i++) {
            static_cast<char*>(buffer)[offset + i] = data[4 * i];
        }
        return {offset + n, tupleOffset + n};
    }

    static WriteResult writeColumnChunk(void* buffer,
                                 const int64_t maxSize,
                                 const size_t tupleOffset,
                                 const std::vector<uint8_t>& header,
                                 const std::shared_ptr<arrow::Array>& array) {
        if (array->type() == arrow::int32()) {
            return writeNumericColumnChunk<int32_t>(buffer, maxSize, tupleOffset, header, array);
        }
        if (array->type() == arrow::float64()) {
            return writeNumericColumnChunk<double>(buffer, maxSize, tupleOffset, header, array);
        }
        if (array->type() == arrow::utf8()) {
            return writeStringColumnChunk(buffer, maxSize, tupleOffset, header, array);
        }
        throw std::logic_error{"unknown type for serialization " + array->type()->ToString()};
    }
};