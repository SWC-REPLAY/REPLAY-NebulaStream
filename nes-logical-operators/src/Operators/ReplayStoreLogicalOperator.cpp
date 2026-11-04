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

#include <Operators/ReplayStoreLogicalOperator.hpp>

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
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

ReplayStoreLogicalOperator::ReplayStoreLogicalOperator(WeakLogicalOperator self, std::string storeName)
    : ManagedByOperator(std::move(self)), storeName(std::move(storeName))
{
}

ReplayStoreLogicalOperator::ReplayStoreLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::string storeName)
    : ManagedByOperator(std::move(self)), child(std::move(child)), storeName(std::move(storeName))
{
    inferLocalSchema();
}

TypedLogicalOperator<ReplayStoreLogicalOperator> ReplayStoreLogicalOperator::create(std::string storeName)
{
    return TypedLogicalOperator<ReplayStoreLogicalOperator>{std::move(storeName)};
}

TypedLogicalOperator<ReplayStoreLogicalOperator> ReplayStoreLogicalOperator::create(LogicalOperator child, std::string storeName)
{
    return TypedLogicalOperator<ReplayStoreLogicalOperator>{std::move(child), std::move(storeName)};
}

std::string_view ReplayStoreLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ReplayStoreLogicalOperator::operator==(const ReplayStoreLogicalOperator& rhs) const
{
    return storeName == rhs.storeName && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

std::string ReplayStoreLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("REPLAY_STORE(opId: {}, storeName: {}, traitSet: {})", opId, storeName, traitSet.explain(verbosity));
    }
    return "REPLAY_STORE";
}

void ReplayStoreLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    outputSchema = unbind(child->getOutputSchema());
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet ReplayStoreLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for ReplayStore, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = withChildrenUnsafe(std::move(children));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> ReplayStoreLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> ReplayStoreLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator ReplayStoreLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>::operator()(
    const TypedLogicalOperator<ReplayStoreLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedReplayStoreLogicalOperator{.operatorId = op.getId(), .storeName = op->getStoreName()});
}

Unreflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<ReplayStoreLogicalOperator>
Unreflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, storeName] = context.unreflect<detail::ReflectedReplayStoreLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("ReplayStoreLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return ReplayStoreLogicalOperator::create(children.at(0), std::move(storeName));
}
}

uint64_t std::hash<NES::ReplayStoreLogicalOperator>::operator()(const NES::ReplayStoreLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getStoreName());
}
