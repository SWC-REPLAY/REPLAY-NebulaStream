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

#include <memory>
#include <string>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Util/Registry.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <StoreRegistry.hpp>

namespace NES
{

using LoweringRuleRegistryReturnType = std::unique_ptr<AbstractLoweringRule>;

struct LoweringRuleRegistryArguments
{
    QueryExecutionConfiguration conf;
    /// The registry of the worker this plan is being compiled for. Lowering a replay store operator materialises the
    /// store here, which is why it cannot come from a process-global.
    std::shared_ptr<StoreManager::StoreRegistry> storeRegistry;
};

class LoweringRuleRegistry
    : public BaseRegistry<LoweringRuleRegistry, std::string, LoweringRuleRegistryReturnType, LoweringRuleRegistryArguments>
{
};
}

#define INCLUDED_FROM_REGISTRY_LOWERING_RULE
#include <LoweringRuleGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_LOWERING_RULE
