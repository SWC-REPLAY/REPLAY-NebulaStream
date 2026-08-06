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

#include <Traits/ReplayReadTrait.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>

#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>

namespace NES
{

ReplayReadTrait::ReplayReadTrait(std::optional<uint64_t> start, std::optional<uint64_t> end) : start(std::move(start)), end(std::move(end))
{
}

const std::type_info& ReplayReadTrait::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return typeid(ReplayReadTrait);
}

std::string_view ReplayReadTrait::getName() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

bool ReplayReadTrait::operator==(const ReplayReadTrait& other) const
{
    return start == other.start && end == other.end;
}

size_t ReplayReadTrait::hash() const
{
    return folly::hash::hash_combine(start.value_or(0), end.value_or(0), start.has_value(), end.has_value());
}

std::string ReplayReadTrait::explain(ExplainVerbosity) const
{
    const auto bound = [](const std::optional<uint64_t>& value) { return value.has_value() ? fmt::format("{}", *value) : "*"; };
    return fmt::format("ReplayReadTrait: [{}, {})", bound(start), bound(end));
}

const std::optional<uint64_t>& ReplayReadTrait::getStart() const
{
    return start;
}

const std::optional<uint64_t>& ReplayReadTrait::getEnd() const
{
    return end;
}

Reflected Reflector<ReplayReadTrait>::operator()(const ReplayReadTrait& trait) const
{
    return reflect(detail::ReflectedReplayReadTrait{.start = trait.start, .end = trait.end});
}

ReplayReadTrait Unreflector<ReplayReadTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [start, end] = context.unreflect<detail::ReflectedReplayReadTrait>(reflected);
    return ReplayReadTrait{start, end};
}

}
