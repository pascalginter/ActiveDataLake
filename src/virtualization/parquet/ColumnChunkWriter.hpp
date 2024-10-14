#pragma once

class ColumnChunkWriter {
    template <typename T>
    static std::vector<uint8_t> writeNumericColumnChunk(const std::shared_ptr<arrow::Array>& array) {
        std::vector<uint8_t> result(array->length() * sizeof(T));
        memcpy(result.data(), array->data()->buffers[1]->data(), array->length() * sizeof(T));
        return result;
    }

    static std::vector<uint8_t> writeStringColumnChunk(const std::shared_ptr<arrow::Array>& array) {
        auto* offsets = reinterpret_cast<const int32_t*>(array->data()->buffers[1]->data());
        int32_t dataSize = offsets[array->length()];
        std::vector<uint8_t> result(dataSize + array->length() * sizeof(int32_t));
        int32_t curr_offset = 0;
        for (int i=0; i!=array->length(); i++) {
            int32_t length = offsets[i+1] - offsets[i];
            memcpy(result.data() + curr_offset, &length, sizeof(int32_t));
            curr_offset += 4;
            memcpy(result.data() + curr_offset, array->data()->buffers[2]->data() + offsets[i], length);
            curr_offset += length;
        }
        return result;
    }
public:
    static std::vector<uint8_t> writeColumnChunk(const std::shared_ptr<arrow::Array>& array) {
        if (array->type() == arrow::int32()) {
            return writeNumericColumnChunk<int32_t>(array);
        }
        if (array->type() == arrow::float64()) {
            return writeNumericColumnChunk<double>(array);
        }
        if (array->type() == arrow::utf8()) {
            return writeStringColumnChunk(array);
        }
        std::cout << "unknown type for serialization" << std::endl;
        exit(1);
    }
};