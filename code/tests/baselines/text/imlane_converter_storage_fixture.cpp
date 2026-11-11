// CPU test double for DuckDB storage only. The converter body and Arrow ABI
// are real source/input; this fixture is not a DuckDB library qualification.
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include "arrow/c/abi.h"

#define D_ASSERT(condition) do { if (!(condition)) throw std::runtime_error(#condition); } while (false)
namespace duckdb {
using idx_t = uint64_t;
using data_ptr_t = uint8_t *;
template<class T> struct NumericLimits {
    static constexpr T Maximum() { return std::numeric_limits<T>::max(); }
};
template<class T, class U> T UnsafeNumericCast(U value) { return static_cast<T>(value); }
struct ConversionException : std::runtime_error {
    template<class... A> ConversionException(const char *message, A...) : std::runtime_error(message) {}
};
using NotImplementedException = ConversionException;
enum class LogicalTypeId { BOOLEAN, TINYINT, SMALLINT, INTEGER, FLOAT, DOUBLE, UTINYINT,
    USMALLINT, UINTEGER, UBIGINT, BIGINT, HUGEINT, UHUGEINT, TIMESTAMP, TIMESTAMP_SEC,
    TIMESTAMP_MS, TIMESTAMP_NS, VARCHAR };
struct Type {
    LogicalTypeId id() const { return LogicalTypeId::VARCHAR; }
    std::string ToString() const { return "VARCHAR"; }
};
struct string_t {
    uint32_t length = 0;
    const char *pointer = nullptr;
    char inlined[12] = {};
    string_t() = default;
    string_t(const char *data, uint32_t size) : length(size) {
        if (size <= sizeof(inlined)) { if (size) std::memcpy(inlined, data, size); }
        else pointer = data;
    }
    const char *GetData() const { return length <= sizeof(inlined) ? inlined : pointer; }
};
struct ValidityMask {
    std::vector<bool> valid;
    explicit ValidityMask(idx_t count) : valid(count, true) {}
    void SetAllValid(idx_t count) { std::fill_n(valid.begin(), count, true); }
    void SetInvalid(idx_t row) { valid.at(row) = false; }
};
struct Vector {
    std::vector<string_t> values;
    ValidityMask validity;
    std::vector<std::unique_ptr<char[]>> heap;
    Vector() : values(2048), validity(2048) {}
    Type GetType() const { return {}; }
};
struct FlatVector {
    template<class T> static T *GetData(Vector &value) { return reinterpret_cast<T *>(value.values.data()); }
    static void SetData(Vector &, data_ptr_t) { throw std::runtime_error("fixed-width path outside this fixture"); }
    static ValidityMask &Validity(Vector &value) { return value.validity; }
    static bool IsNull(Vector &value, idx_t row) { return !value.validity.valid.at(row); }
};
struct StringVector {
    static string_t AddStringOrBlob(Vector &value, const char *data, idx_t length) {
        if (length <= 12) return string_t(data, length);
        auto storage = std::make_unique<char[]>(length);
        std::memcpy(storage.get(), data, length);
        auto pointer = storage.get();
        value.heap.push_back(std::move(storage));
        return string_t(pointer, length);
    }
};
union arrow_string_view_t {
    struct { int32_t length; char data[12]; } inlined;
    struct { int32_t length; char prefix[4]; int32_t buffer_index; int32_t offset; } ref;
    int32_t Length() const { return inlined.length; }
    bool IsInline() const { return Length() <= 12; }
    const char *GetInlineData() const { return inlined.data; }
    int32_t GetBufferIndex() const { return ref.buffer_index; }
    int32_t GetOffset() const { return ref.offset; }
};
}

// Generated directly from the hash-bound patched author header, never a
// separately maintained converter implementation.
#include "converter_body.inc"

extern "C" {
void *new_vector() { return new duckdb::Vector(); }
void delete_vector(void *value) { delete static_cast<duckdb::Vector *>(value); }
int convert(ArrowArray *array, ArrowSchema *schema, void *value) {
    try {
        duckdb::ConverArrowAarrayToVector(array, schema, *static_cast<duckdb::Vector *>(value));
        return 0;
    } catch (...) { return 1; }
}
bool is_null(void *value, uint64_t row) {
    return duckdb::FlatVector::IsNull(*static_cast<duckdb::Vector *>(value), row);
}
uint32_t string_size(void *value, uint64_t row) {
    return static_cast<duckdb::Vector *>(value)->values.at(row).length;
}
const void *string_data(void *value, uint64_t row) {
    return static_cast<duckdb::Vector *>(value)->values.at(row).GetData();
}
}
