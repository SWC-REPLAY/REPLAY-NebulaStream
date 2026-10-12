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

#include <CompressionCodec.hpp>

#include <memory>
#include <stdexcept>
#include <string>

#include <zstd.h>
#include <snappy.h>

namespace NES
{

CompressionCodec::CompressionCodec(Type type, int level) : type(type), level(level)
{
}

std::vector<uint8_t> CompressionCodec::compress(const uint8_t* src, size_t len) const
{

    switch (type)
    {
        case Type::ZSTD:
        {
            // Worst-case output size for len input bytes.
            const size_t bound = ZSTD_compressBound(len);
            std::vector<uint8_t> out(bound);

            const size_t written = ZSTD_compress(out.data(), bound, src, len, level);
            if (ZSTD_isError(written) != 0U)
            {
                throw std::runtime_error(std::string("zstd compress failed: ") + ZSTD_getErrorName(written));
            }

            out.resize(written); // shrink to what was actually produced
            return out;
        }
        case Type::SNAPPY:
        {
            const size_t bound = snappy::MaxCompressedLength(len);
            std::vector<uint8_t> out(bound);
            size_t outLen = 0;
            snappy::RawCompress(reinterpret_cast<const char*>(src), len, reinterpret_cast<char*>(out.data()), &outLen);
            out.resize(outLen);
            return out;

        }

    }
    throw std::runtime_error("CompressionCodec::compress: unknown codec type");
}

std::vector<uint8_t> CompressionCodec::decompress(const uint8_t* src, size_t compressedLen, size_t uncompressedLen) const
{
    switch (type)
    {
        case Type::ZSTD:
        {
            std::vector<uint8_t> out(uncompressedLen);

            const size_t written = ZSTD_decompress(out.data(), uncompressedLen, src, compressedLen);
            if (ZSTD_isError(written) != 0U)
            {
                throw std::runtime_error(std::string("zstd decompress failed: ") + ZSTD_getErrorName(written));
            }
            if (written != uncompressedLen)
            {
                throw std::runtime_error("zstd decompress: size mismatch (file may be corrupt)");
            }
            return out;
        }

        case Type::SNAPPY:
        {
            std::vector<uint8_t> out(uncompressedLen);
            if (!snappy::RawUncompress(reinterpret_cast<const char*>(src), compressedLen, reinterpret_cast<char*>(out.data())))
            {
                throw std::runtime_error("snappy decompress failed.");
            }
            return out;
        }
    }
    throw std::runtime_error("CompressionCodec::decompress: unknown codec type");
}

std::string_view CompressionCodec::name() const noexcept
{
    switch (type)
    {
        case Type::ZSTD:
            return "zstd";
        case Type::SNAPPY:
            return "snappy";
    }
    return "unknown";
}

}
