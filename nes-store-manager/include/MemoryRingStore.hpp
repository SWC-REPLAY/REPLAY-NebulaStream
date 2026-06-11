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
#include "DataTypes/Schema.hpp"
#include "Runtime/TupleBuffer.hpp"

namespace NES::StoreManager
{
class MemoryRingStore
{
public:
    explicit MemoryRingStore(Schema schema);

    void open();
    void close();
    void flush();

    void write(TupleBuffer buffer, const Schema& schema);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    [[nodiscard]] bool hasMore() const;
    [[nodiscard]] size_t size() const;

    [[nodiscard]] std::string_view typeName() const noexcept { return "MemoryRingStore"; }

private:
    static constexpr size_t RINGBUFFER_SIZE = 2;
    Schema schema;
    std::array<TupleBuffer, RINGBUFFER_SIZE> ringBuffer;
    std::array<TupleBuffer, RINGBUFFER_SIZE>::iterator readIterator;
    std::array<TupleBuffer, RINGBUFFER_SIZE>::iterator writeIterator;
};
}
