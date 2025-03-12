#pragma once

class ColumnChunkWriter {
    template <typename T>
    static void writeNumericColumnChunk(const std::vector<uint8_t>& header,
                                                        const std::shared_ptr<arrow::Array>& array,
                                                        std::vector<uint8_t>& vec) {
        memcpy(vec.data(), header.data(), header.size());
        memcpy(vec.data() + header.size(), array->data()->buffers[1]->data(), array->length() * sizeof(T));
    }

    static void writeStringColumnChunk(const std::vector<uint8_t>& header,
                                       const std::shared_ptr<arrow::Array>& array,
                                       std::vector<uint8_t>& vec) {
        const uint8_t* src = array->data()->buffers[2]->data();
        auto* offsets = reinterpret_cast<const int32_t*>(array->data()->buffers[1]->data());
        memcpy(vec.data(), header.data(), header.size());
        int32_t curr_offset = header.size();
        const int64_t n = array->length();
        for (int64_t i=0; i!=n; i++) {
            int32_t length = offsets[i+1] - offsets[i];
            memcpy(vec.data() + curr_offset, &length, sizeof(int32_t));
            curr_offset += 4;
            memcpy(vec.data() + curr_offset, src + offsets[i], length);
            curr_offset += length;
            assert(vec.size() >= curr_offset);
        }
    }
public:
    static void writeDictionaryEncodedChunk(const std::vector<uint8_t>& header,
                                            const std::shared_ptr<arrow::Array>& array,
                                            std::vector<uint8_t>& vec,
                                            const uint8_t bitLength,
                                            const int32_t offset) {
        const auto* data = reinterpret_cast<const int32_t *>(array->data()->buffers[1]->data());
        const uint64_t n = array->length();
        memcpy(vec.data(), header.data(), header.size());
        size_t currentByte = header.size();
        uint64_t bitBuffer = 0;
        int bitsInBuffer = 0;

        // Create a mask to ensure we only pack the lower 'bitLength' bits.
        const uint32_t mask = (bitLength == 32 ? 0xFFFFFFFFu : ((1u << bitLength) - 1));

        for (uint64_t i = 0; i < n; i++) {
            // Compute the value to pack.
            uint32_t v = static_cast<uint32_t>(data[i] + offset) & mask;

            // Append v's bits into the buffer.
            bitBuffer |= (static_cast<uint64_t>(v) << bitsInBuffer);
            bitsInBuffer += bitLength;

            // Flush full bytes from the buffer.
            while (bitsInBuffer >= 8) {
                vec[currentByte++] = static_cast<uint8_t>(bitBuffer & 0xFF);
                bitBuffer >>= 8;
                bitsInBuffer -= 8;
            }
        }

        // Flush any remaining bits.
        if (bitsInBuffer > 0) {
            vec[currentByte++] = static_cast<uint8_t>(bitBuffer & 0xFF);
        }
    }

    static void writeColumnChunk(const std::vector<uint8_t>& header,
                                 const std::shared_ptr<arrow::Array>& array,
                                 std::vector<uint8_t>& vec) {
        if (array->type() == arrow::int32()) {
            return writeNumericColumnChunk<int32_t>(header, array, vec);
        }
        if (array->type() == arrow::float64()) {
            return writeNumericColumnChunk<double>(header, array, vec);
        }
        if (array->type() == arrow::utf8()) {
            return writeStringColumnChunk(header, array, vec);
        }
        throw std::logic_error{"unknown type for serialization " + array->type()->ToString()};
    }
};