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