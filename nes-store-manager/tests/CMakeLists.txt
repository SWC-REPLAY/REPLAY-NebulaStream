# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(store-concurrency-tests ConcurrencyTests.cpp)
target_link_libraries(store-concurrency-tests nes-store-manager)

add_nes_test(store-compressed-tests CompressedStoreTest.cpp)
target_link_libraries(store-compressed-tests nes-store-manager nes-test-util)

add_nes_test(store-file-to-compressed-chain-tests FileToCompressedChainTest.cpp)
target_link_libraries(store-file-to-compressed-chain-tests nes-store-manager nes-test-util)
add_nes_test(store-segmented-file-tests SegmentedFileStoreTest.cpp)
target_link_libraries(store-segmented-file-tests nes-store-manager nes-test-util)

add_nes_test(store-memory-range-tests MemoryStoreRangeTest.cpp)
target_link_libraries(store-memory-range-tests nes-store-manager nes-test-util)

add_nes_test(store-compressed-chain-config-tests CompressedChainConfigTest.cpp)
target_link_libraries(store-compressed-chain-config-tests nes-store-manager nes-test-util)
