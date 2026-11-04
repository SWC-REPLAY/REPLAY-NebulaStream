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

#include <Operators/UdbRecordingLogicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

UdbRecordingLogicalOperator::UdbRecordingLogicalOperator(WeakLogicalOperator self, std::string traceName, std::string traceSize)
    : ManagedByOperator(std::move(self)), traceName(std::move(traceName)), traceSize(std::move(traceSize))
{
}

UdbRecordingLogicalOperator::UdbRecordingLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, std::string traceName, std::string traceSize)
    : ManagedByOperator(std::move(self)), child(std::move(child)), traceName(std::move(traceName)), traceSize(std::move(traceSize))
{
    inferLocalSchema();
}

TypedLogicalOperator<UdbRecordingLogicalOperator> UdbRecordingLogicalOperator::create(std::string traceName, std::string traceSize)
{
    return TypedLogicalOperator<UdbRecordingLogicalOperator>{std::move(traceName), std::move(traceSize)};
}

TypedLogicalOperator<UdbRecordingLogicalOperator>
UdbRecordingLogicalOperator::create(LogicalOperator child, std::string traceName, std::string traceSize)
{
    return TypedLogicalOperator<UdbRecordingLogicalOperator>{std::move(child), std::move(traceName), std::move(traceSize)};
}

std::string_view UdbRecordingLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UdbRecordingLogicalOperator::operator==(const UdbRecordingLogicalOperator& rhs) const
{
    return traceName == rhs.traceName && traceSize == rhs.traceSize && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

std::string UdbRecordingLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "UDB_RECORDING(opId: {}, traceName: {}, traceSize: {}, traitSet: {})", opId, traceName, traceSize, traitSet.explain(verbosity));
    }
    return "UDB_RECORDING";
}

void UdbRecordingLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    outputSchema = unbind(child->getOutputSchema());
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet UdbRecordingLogicalOperator::getTraitSet() const
{
    return traitSet;
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for UdbRecording, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = withChildrenUnsafe(std::move(children));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> UdbRecordingLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> UdbRecordingLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator UdbRecordingLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<UdbRecordingLogicalOperator>>::operator()(
    const TypedLogicalOperator<UdbRecordingLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedUdbRecordingLogicalOperator{
            .operatorId = op.getId(), .traceName = op->getTraceName(), .traceSize = op->getTraceSize()});
}

Unreflector<TypedLogicalOperator<UdbRecordingLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<UdbRecordingLogicalOperator>
Unreflector<TypedLogicalOperator<UdbRecordingLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, traceName, traceSize] = context.unreflect<detail::ReflectedUdbRecordingLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("UdbRecordingLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return UdbRecordingLogicalOperator::create(children.at(0), std::move(traceName), std::move(traceSize));
}
}

uint64_t std::hash<NES::UdbRecordingLogicalOperator>::operator()(const NES::UdbRecordingLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getTraceName());
}
