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
        auto* offsets = reinterpret_cast<const int32_t*>(array->data()->buffers[1]->data());
        memcpy(vec.data(), header.data(), header.size());
        int32_t curr_offset = header.size();
        for (int i=0; i!=array->length(); i++) {
            int32_t length = offsets[i+1] - offsets[i];
            memcpy(vec.data() + curr_offset, &length, sizeof(int32_t));
            curr_offset += 4;
            memcpy(vec.data() + curr_offset, array->data()->buffers[2]->data() + offsets[i], length);
            curr_offset += length;
        }}
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
        std::cout << "unknown type for serialization" << std::endl;
        std::cout << array->type()->ToString() << std::endl;
        exit(1);
    }
};