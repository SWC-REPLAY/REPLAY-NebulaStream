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
#include <utility>
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

/// Persists every row into the named store and passes it downstream unchanged.
class ReplayStoreLogicalOperator : public ManagedByOperator
{
public:
    ReplayStoreLogicalOperator(WeakLogicalOperator self, std::string storeName);
    ReplayStoreLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::string storeName);

    static TypedLogicalOperator<ReplayStoreLogicalOperator> create(std::string storeName);
    static TypedLogicalOperator<ReplayStoreLogicalOperator> create(LogicalOperator child, std::string storeName);

    [[nodiscard]] const std::string& getStoreName() const { return storeName; }

    [[nodiscard]] bool operator==(const ReplayStoreLogicalOperator& rhs) const;

    [[nodiscard]] ReplayStoreLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ReplayStoreLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ReplayStoreLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] ReplayStoreLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "ReplayStore";
    std::optional<LogicalOperator> child;
    std::string storeName;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
};

namespace detail
{
struct ReflectedReplayStoreLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::string storeName;
};
}

template <>
struct Reflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<ReplayStoreLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<ReplayStoreLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ReplayStoreLogicalOperator>);
}

template <>
struct std::hash<NES::ReplayStoreLogicalOperator>
{
    uint64_t operator()(const NES::ReplayStoreLogicalOperator& op) const noexcept;
};
