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

#include <cstdint>
#include <string>

#include <Time/Timestamp.hpp>

namespace NES
{

/// Defines a time range for filtered reads from a store.
/// The range is [start, end) by default (inclusive start, exclusive end).
struct TimeRange
{
    std::string fieldName; /// Schema field to filter on (e.g. "ts")
    Timestamp start{Timestamp(Timestamp::INITIAL_VALUE)}; /// Lower bound (inclusive)
    Timestamp end{Timestamp(Timestamp::INVALID_VALUE)}; /// Upper bound (exclusive), INVALID_VALUE = no upper bound

    /// Returns true if no filtering is needed (range covers all possible timestamps).
    [[nodiscard]] bool isUnbounded() const
    {
        return start.getRawValue() == Timestamp::INITIAL_VALUE && end.getRawValue() == Timestamp::INVALID_VALUE;
    }

    /// Returns true if a value falls within this range: start <= value < end
    [[nodiscard]] bool contains(Timestamp value) const { return value >= start && value < end; }

    /// Returns true if a buffer with the given min/max timestamps could contain values in this range.
    [[nodiscard]] bool overlaps(Timestamp bufferMin, Timestamp bufferMax) const { return bufferMax >= start && bufferMin < end; }
};

}
