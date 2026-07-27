# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(NES_UNDOLR_DIR "$ENV{UNDO_SUITE_DIR}/undolr" CACHE PATH "Undo LiveRecorder (undolr) directory")

if (NOT EXISTS ${NES_UNDOLR_DIR}/undolr.h)
    message(FATAL_ERROR "undolr.h not found under NES_UNDOLR_DIR=${NES_UNDOLR_DIR}. "
            "Pass -DNES_UNDOLR_DIR=<undo-suite>/undolr or export UNDO_SUITE_DIR.")
endif ()

if (NOT NES_ARCH STREQUAL "x64")
    message(FATAL_ERROR "LiveRecorder is only wired up for x86-64, but NES_ARCH is ${NES_ARCH}.")
endif ()

add_subdirectory(src)
get_source(nes-udb NES_UDB_SOURCE_FILES)

add_library(nes-udb ${NES_UDB_SOURCE_FILES})

# Both the LiveRecorder header and its library stay PRIVATE: no NES header includes undolr.h, so
# the Undo dependency cannot leak into any consumer of this target.
target_link_libraries(nes-udb PRIVATE nes-common ${NES_UNDOLR_DIR}/libundolr_pic_x64.a)
target_include_directories(nes-udb PRIVATE ${NES_UNDOLR_DIR})
target_include_directories(nes-udb PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
