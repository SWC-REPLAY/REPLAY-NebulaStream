# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Plain steady_clock, not benchmark::benchmark as nes-common/benchmarks uses: Google Benchmark
# repeats until a measurement stabilises, which on a 42.7M-row store write is many minutes at full
# load. The repeat count here is explicit.
add_executable(lossy-store-benchmark LossyStoreBenchmark.cpp)
target_link_libraries(lossy-store-benchmark PRIVATE nes-store-manager)
