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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Records the execution of its pipeline with udb and passes rows through unchanged.
/// Without a trace name, udb generates one.
/// Configuration for UDB recording; doubles as the operator's reflected payload.
/// Adding a field here also requires forwarding it in LowerToPhysicalUdbRecording.
struct UdbRecordingOptions
{
    std::optional<std::string> traceName;
    std::optional<std::string> traceSize;

    bool operator==(const UdbRecordingOptions& rhs) const = default;
};

class UdbRecordingLogicalOperator : public ManagedByOperator
{
public:
    UdbRecordingLogicalOperator(WeakLogicalOperator self, std::optional<std::string> traceName);
    UdbRecordingLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::optional<std::string> traceName);

    static TypedLogicalOperator<UdbRecordingLogicalOperator> create(std::optional<std::string> traceName);
    static TypedLogicalOperator<UdbRecordingLogicalOperator> create(LogicalOperator child, std::optional<std::string> traceName);

    [[nodiscard]] const UdbRecordingOptions& getOptions() const { return options; }

    [[nodiscard]] bool operator==(const UdbRecordingLogicalOperator& rhs) const;

    [[nodiscard]] UdbRecordingLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UdbRecordingLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] UdbRecordingLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] UdbRecordingLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "UdbRecording";
    std::optional<LogicalOperator> child;
    std::optional<std::string> traceName;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
};

namespace detail
{
struct ReflectedUdbRecordingLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::optional<std::string> traceName;
};
}

template <>
struct Reflector<TypedLogicalOperator<UdbRecordingLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<UdbRecordingLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<UdbRecordingLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<UdbRecordingLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<UdbRecordingLogicalOperator>);
}

template <>
struct std::hash<NES::UdbRecordingLogicalOperator>
{
    uint64_t operator()(const NES::UdbRecordingLogicalOperator& op) const noexcept;
};
