/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace NES
{

// A lossless codec: decompress(compress(x)) == x. The only place that knows the compression library (zstd).
class CompressionCodec
{
public:
    // The concrete algorithm; its value is stored in each segment header. Never renumber.
    enum class Type : uint8_t
    {
        ZSTD = 1,
        SNAPPY = 2,
    };

    // level: zstd 1 (fastest) .. 22 (smallest), 3 is default; used by compression only.
    explicit CompressionCodec(Type type = Type::ZSTD, int level = 3);

    /// Compress `len` bytes starting at `src`. Returns the compressed bytes.
    [[nodiscard]] std::vector<uint8_t> compress(const uint8_t* src, size_t len) const;

    // Decompress a blob; uncompressedLen is the original size (stored in the segment header).
    [[nodiscard]] std::vector<uint8_t> decompress(const uint8_t* src, size_t compressedLen, size_t uncompressedLen) const;

    [[nodiscard]] Type getType() const { return type; }
    [[nodiscard]] uint8_t getTypeId() const { return static_cast<uint8_t>(type); }
    [[nodiscard]] int getLevel() const { return level; }

    /// Human-readable name, e.g. "zstd".
    [[nodiscard]] std::string_view name() const noexcept;

private:
    Type type;
    int level;
};

}
