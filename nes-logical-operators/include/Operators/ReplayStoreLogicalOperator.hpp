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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Logical operator that persists rows to a binary file while passing them downstream unchanged.
class ReplayStoreLogicalOperator : public ManagedByOperator
{
public:
    ReplayStoreLogicalOperator(
        LogicalFunction tsExtractionFunction, const Windowing::TimeUnit& unit, DescriptorConfig::Config validatedConfig)
        : ManagedByOperator(WeakLogicalOperator{})
        , tsExtractionFunction(std::move(tsExtractionFunction))
        , unit(unit)
        , config(std::move(validatedConfig))
    {
    }

    LogicalFunction tsExtractionFunction;
    Windowing::TimeUnit unit;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] ReplayStoreLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ReplayStoreLogicalOperator withTraitSet(TraitSet ts) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] bool operator==(const ReplayStoreLogicalOperator& rhs) const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;
    [[nodiscard]] ReplayStoreLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    [[nodiscard]] const DescriptorConfig::Config& getConfig() const { return config; }

    [[nodiscard]] ReplayStoreLogicalOperator withConfig(DescriptorConfig::Config validatedConfig) const;

    struct ConfigParameters
    {
        /// Empty until StoreRegistrationRule assigns one. The parser cannot name a store: names must be unique across
        /// queries, and once the optimizer places store operators it also decides how many of them there are.
        static inline const DescriptorConfig::ConfigParameter<std::string> STORE_NAME{
            "store_name",
            std::string{},
            [](const std::unordered_map<std::string, std::string>& cfg) { return DescriptorConfig::tryGet(STORE_NAME, cfg); }};

        /// The store parameters default to "unset" rather than to concrete values on purpose. Config validation fills in
        /// a default for every parameter the query did not mention, so a concrete default here would be indistinguishable
        /// from a value the user asked for, and would silently mask the worker-level replay configuration that lowering
        /// falls back to. Empty string and zero mean "not given by the query".
        static inline const DescriptorConfig::ConfigParameter<std::string> MEMORY_BUFFER_SIZE{
            "memory_buffer_size",
            std::string{},
            [](const std::unordered_map<std::string, std::string>& cfg) { return DescriptorConfig::tryGet(MEMORY_BUFFER_SIZE, cfg); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STORE_ORDER{
            "store_order",
            std::string{},
            [](const std::unordered_map<std::string, std::string>& cfg) { return DescriptorConfig::tryGet(STORE_ORDER, cfg); }};

        static inline const DescriptorConfig::ConfigParameter<uint64_t> MAX_BUFFER_COUNT{
            "max_buffer_count",
            uint64_t{0},
            [](const std::unordered_map<std::string, std::string>& cfg) { return DescriptorConfig::tryGet(MAX_BUFFER_COUNT, cfg); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(STORE_NAME, MEMORY_BUFFER_SIZE, STORE_ORDER, MAX_BUFFER_COUNT);
    };

    static DescriptorConfig::Config validateAndFormatConfig(std::unordered_map<std::string, std::string> configPairs);

private:
    static constexpr std::string_view NAME = "ReplayStore";
    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    DescriptorConfig::Config config;
};

template <>
struct Reflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<ReplayStoreLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>
{
    TypedLogicalOperator<ReplayStoreLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ReplayStoreLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedStoreLogicalOperator
{
    DescriptorConfig::Config config;
    std::optional<LogicalFunction> onField;
    Windowing::TimeUnit timeUnit;
};
}
