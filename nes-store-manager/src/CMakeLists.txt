# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-store-manager
        ReplayStoreFormat.cpp
        BinaryStoreWriter.cpp
        ReplayStoreReader.cpp
        StoreRegistry.cpp
        MemoryStore.cpp
        FileStore.cpp
        MemoryToFileTransformation.cpp
        lossless/CompressionCodec.cpp
        lossless/LosslessStore.cpp
        lossless/FileToCompressedTransformation.cpp
        lossy/ColumnCodec.cpp
        lossy/Primitives.cpp
        lossy/Quantize.cpp
        lossy/LossyGroupFormat.cpp
)

add_plugin(MemoryStore StoreType MemoryStore.cpp)
add_plugin(FileStore StoreType FileStore.cpp)
add_plugin(LossyStore StoreType lossy/LossyStore.cpp)
add_plugin(MemoryStore_to_FileStore StoreTransformation MemoryToFileTransformation.cpp)
add_plugin(FileStore_to_LosslessStore StoreTransformation lossless/FileToCompressedTransformation.cpp)
add_plugin(FileStore_to_LossyStore StoreTransformation lossy/FileToLossyTransformation.cpp)

