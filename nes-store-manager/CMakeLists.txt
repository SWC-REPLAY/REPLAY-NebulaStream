# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include registrar generation functions
add_library(nes-store-manager STATIC)
set_target_properties(nes-store-manager PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(nes-store-manager PUBLIC nes-common nes-data-types nes-memory nes-schema)
target_compile_definitions(nes-store-manager PRIVATE STORE_MANAGER_WORKING_DIR="${CMAKE_CURRENT_BINARY_DIR}/")
target_include_directories(nes-store-manager PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

create_runtime_registry(StoreTransformation nes-store-manager
        ENTRY_TEMPLATE "NES::StoreManager::\${PLUGIN_NAME}{}"
        HEADER_TEMPLATE "\${PLUGIN_NAME}.hpp")

add_subdirectory(src)
get_source(nes-store-manager NES_STORE_MANAGER_SOURCE_FILES)
target_sources(nes-store-manager PRIVATE ${NES_STORE_MANAGER_SOURCE_FILES})

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-store-manager REUSE_FROM nes-common)
endif ()
