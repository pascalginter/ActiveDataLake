#pragma once
// credit duckdb

#define NEXT_INTEGER_FIELD 0x15
#define DATA_PAGE_V1 0x00
#define NEXT_STRUCT_FIELD 0x2c
#define END_STRUCT 0x00
#define PLAIN_ENCODING 0x00
#define RLE_ENCODING 0x03


class ParquetUtils {
public:
    static uint8_t GetVarintSize(uint32_t val) {
        uint8_t res = 0;
        do {
            val >>= 7;
            res++;
        } while (val != 0);
        return res;
    }

    static uint64_t GetZigZag(uint64_t val) {
        return (val >> 63) ^ (val << 1);
    }

private:
    static void appendZigZagVarint(std::vector<uint8_t>& v, uint64_t val) {
        do {
            uint8_t byte = val & 127;
            val >>= 7;
            if (val != 0) {
                byte |= 128;
            }
            v.push_back(byte);
        } while (val != 0);
    }
public:
    // {0x15, 0x00, 0x15, 0x90, 0x80, 0x78, 0x15, 0x90, 0x80, 0x78, 0x2c, 0x15, 0x80, 0x80, 0x0f, 0x15, 0x00, 0x15, 0x06, 0x15, 0x06, 0x00, 0x00};
    // 0x15, 0x00 -> data page
    // 0x15, 0x90, 0x80, 0x78 -> uncompressed size
    // 0x15, 0x90, 0x80, 0x78 -> compressed size
    // 0x2c -> start struct
    // 0x15, 0x80, 0x80, 0x0f -> num_values
    // 0x15, 0x00 -> encoding
    // 0x15, 0x06 -> definition_level_encoding 0x06 (uleb128) = 3 -> rle/bit packing
    // 0x15, 0x06 -> repetition_level_encoding
    // 0x00 -> end data page
    // 0x00 -> end page
    // 0x04, 0x00, 0x00, 0x00 -> 4 byte definition levels
    // 0x80, 0x80, 0x0f -> uleb128 lenght = num_tuples
    // 0x01 -> repeated value
    static std::vector<uint8_t> writePageWithoutData(uint64_t uncompressed_size, uint64_t num_values){
        // Declare data page
        std::vector<uint8_t> result{NEXT_INTEGER_FIELD, DATA_PAGE_V1};
        // Write uncompressed size
        result.push_back(NEXT_INTEGER_FIELD);
        appendZigZagVarint(result, GetZigZag(uncompressed_size));
        // Write compressed size
        result.push_back(NEXT_INTEGER_FIELD);
        appendZigZagVarint(result, GetZigZag(uncompressed_size));
        // Start struct
        result.push_back(NEXT_STRUCT_FIELD);
        // Write num_values
        result.push_back(NEXT_INTEGER_FIELD);
        appendZigZagVarint(result, GetZigZag(num_values));
        // Write encoding
        result.push_back(NEXT_INTEGER_FIELD);
        appendZigZagVarint(result, GetZigZag(PLAIN_ENCODING));
        // Write definition level encoding
        result.push_back(NEXT_INTEGER_FIELD);
        appendZigZagVarint(result, GetZigZag(RLE_ENCODING));
        // Write repetition level encoding
        result.push_back(NEXT_INTEGER_FIELD);
        appendZigZagVarint(result, GetZigZag(RLE_ENCODING));
        // End data page
        result.push_back(END_STRUCT);
        // End page
        result.push_back(END_STRUCT);

        // Write length of rle run
        result.push_back((GetVarintSize(num_values) + 1));
        for (int i=0; i!=3; i++) result.push_back(0x00);

        appendZigZagVarint(result, num_values << 1);
        result.push_back(0x01);

        // for (const int a : result) std::cout << std::hex << a << " ";
        // std::cout << std::dec << std::endl;

        return result;
    }
};