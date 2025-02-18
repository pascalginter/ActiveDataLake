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
            uint8_t byte = val & 0x7F;
            val >>= 7;
            if (val != 0) {
                byte |= 0x80;
            }
            v.push_back(byte);
        } while (val != 0);
    }
public:
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
        result.push_back((GetVarintSize(num_values << 1) + 1));
        for (int i=0; i!=3; i++) result.push_back(0x00);

        appendZigZagVarint(result, num_values << 1);
        result.push_back(0x01);

        return result;
    }
};