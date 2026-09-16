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

/// A lossless codec = compressor + decompressor.
///
/// "Lossless" means the guarantee: decompress(compress(x)) == x, byte for byte.
/// This class is the ONLY place that knows about the underlying compression
/// library (currently zstd). Everything else in the store just asks it to
/// "compress these bytes" / "decompress these bytes" and never sees zstd.
///
/// To add another codec later (e.g. Snappy), add a Type enum value and one
/// case in each switch in the .cpp — nothing else changes.
class CompressionCodec
{
public:
    /// The concrete algorithm. The numeric value is stored on disk (in each
    /// segment header) so a reader knows how to decompress. Never renumber
    /// existing values once files exist in the wild.
    enum class Type : uint8_t
    {
        ZSTD = 1,
        SNAPPY = 2,   // added in a later step
    };

    /// @param type  which algorithm to use
    /// @param level compression level (zstd: 1 = fastest .. 22 = smallest; 3 is the zstd default).
    ///              Ignored by decompression — only compression uses it.
    explicit CompressionCodec(Type type = Type::ZSTD, int level = 3);

    /// Compress `len` bytes starting at `src`. Returns the compressed bytes.
    [[nodiscard]] std::vector<uint8_t> compress(const uint8_t* src, size_t len) const;

    /// Decompress a blob back to its original bytes.
    /// @param uncompressedLen the ORIGINAL size (we store this in the segment header,
    ///                        so we know exactly how big the output buffer must be).
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
