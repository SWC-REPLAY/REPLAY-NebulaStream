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
#include <optional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Marks a source reference that came from a `FOR EVENT_TIME` clause, carrying the requested event-time range.
///
/// The parser attaches this; it does not resolve anything. Deciding *which* store can answer the range needs the store
/// catalog, so that happens in a semantic rule. The range is normalised to half-open [start, end) here so the various
/// surface syntaxes (BETWEEN, FROM..TO, CONTAINED IN, AS OF, ALL) stop being distinguishable downstream.
class ReplayReadTrait final
{
public:
    static constexpr std::string_view NAME = "ReplayRead";

    /// An empty bound means unbounded on that side; both empty is `TIMESTAMP ALL`.
    ReplayReadTrait(std::optional<uint64_t> start, std::optional<uint64_t> end);

    [[nodiscard]] const std::type_info& getType() const;
    [[nodiscard]] std::string_view getName() const;
    bool operator==(const ReplayReadTrait& other) const;
    [[nodiscard]] size_t hash() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const std::optional<uint64_t>& getStart() const;
    [[nodiscard]] const std::optional<uint64_t>& getEnd() const;

private:
    std::optional<uint64_t> start;
    std::optional<uint64_t> end;

    friend Reflector<ReplayReadTrait>;
};

template <>
struct Reflector<ReplayReadTrait>
{
    Reflected operator()(const ReplayReadTrait& trait) const;
};

template <>
struct Unreflector<ReplayReadTrait>
{
    ReplayReadTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<ReplayReadTrait>);

}

namespace NES::detail
{
struct ReflectedReplayReadTrait
{
    std::optional<uint64_t> start;
    std::optional<uint64_t> end;
};
}
