# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
get_source(nes-udb NES_UDB_SOURCE_FILES)

add_library(nes-udb ${NES_UDB_SOURCE_FILES})

target_link_libraries(nes-udb PRIVATE nes-common)

target_compile_definitions(nes-udb PRIVATE
    UDB_RECORDING_STORE_DIR="${CMAKE_BINARY_DIR}/udb-rec/")

target_include_directories(nes-udb PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-udb REUSE_FROM nes-common)
endif ()
