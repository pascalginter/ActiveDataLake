#pragma once

class ColumnChunkWriter {
    template <typename T>
    static std::vector<uint8_t> writeNumericColumnChunk(const std::vector<uint8_t>& header,
                                                        const std::shared_ptr<arrow::Array>& array) {
        std::vector<uint8_t> result(header.size() + array->length() * sizeof(T));
        memcpy(result.data(), header.data(), header.size());
        memcpy(result.data() + header.size(), array->data()->buffers[1]->data(), array->length() * sizeof(T));
        return result;
    }

    static std::vector<uint8_t> writeStringColumnChunk(const std::vector<uint8_t>& header,
                                                       const std::shared_ptr<arrow::Array>& array,
                                                       uint64_t expected_size) {
        auto* offsets = reinterpret_cast<const int32_t*>(array->data()->buffers[1]->data());
        int32_t dataSize = offsets[array->length()];
        std::vector<uint8_t> result(header.size() + dataSize + array->length() * sizeof(int32_t));
        assert(result.size() == expected_size);
        memcpy(result.data(), header.data(), header.size());
        int32_t curr_offset = header.size();
        for (int i=0; i!=array->length(); i++) {
            int32_t length = offsets[i+1] - offsets[i];
            memcpy(result.data() + curr_offset, &length, sizeof(int32_t));
            curr_offset += 4;
            memcpy(result.data() + curr_offset, array->data()->buffers[2]->data() + offsets[i], length);
            curr_offset += length;
        }
        assert(curr_offset == result.size());
        return result;
    }
public:
    static std::vector<uint8_t> writeColumnChunk(const std::vector<uint8_t>& header,
                                                 const std::shared_ptr<arrow::Array>& array,
                                                 uint64_t expected_size) {
        if (array->type() == arrow::int32()) {
            return writeNumericColumnChunk<int32_t>(header, array);
        }
        if (array->type() == arrow::float64()) {
            return writeNumericColumnChunk<double>(header, array);
        }
        if (array->type() == arrow::utf8()) {
            return writeStringColumnChunk(header, array, expected_size);
        }
        std::cout << "unknown type for serialization" << std::endl;
        std::cout << array->type()->ToString() << std::endl;
        exit(1);
    }
};