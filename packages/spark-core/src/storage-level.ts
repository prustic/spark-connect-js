/**
 * Storage level for controlling how DataFrames are cached.
 *
 * Mirrors the Java/Scala/Python StorageLevel class with the same
 * predefined constants.
 *
 * @see org.apache.spark.storage.StorageLevel
 */
export interface StorageLevel {
  useDisk: boolean;
  useMemory: boolean;
  useOffHeap: boolean;
  deserialized: boolean;
  replication: number;
}

/** Do not cache. */
export const NONE: StorageLevel = {
  useDisk: false,
  useMemory: false,
  useOffHeap: false,
  deserialized: false,
  replication: 1,
};

/** Cache deserialized in JVM heap memory only. */
export const MEMORY_ONLY: StorageLevel = {
  useDisk: false,
  useMemory: true,
  useOffHeap: false,
  deserialized: true,
  replication: 1,
};

/** Cache serialized in JVM heap memory only. */
export const MEMORY_ONLY_SER: StorageLevel = {
  useDisk: false,
  useMemory: true,
  useOffHeap: false,
  deserialized: false,
  replication: 1,
};

/** Cache deserialized in JVM heap memory, spilling to disk. */
export const MEMORY_AND_DISK: StorageLevel = {
  useDisk: true,
  useMemory: true,
  useOffHeap: false,
  deserialized: true,
  replication: 1,
};

/** Cache serialized in JVM heap memory, spilling to disk. */
export const MEMORY_AND_DISK_SER: StorageLevel = {
  useDisk: true,
  useMemory: true,
  useOffHeap: false,
  deserialized: false,
  replication: 1,
};

/** Cache on disk only. */
export const DISK_ONLY: StorageLevel = {
  useDisk: true,
  useMemory: false,
  useOffHeap: false,
  deserialized: false,
  replication: 1,
};

/** Cache deserialized in JVM heap memory with 2x replication. */
export const MEMORY_ONLY_2: StorageLevel = {
  useDisk: false,
  useMemory: true,
  useOffHeap: false,
  deserialized: true,
  replication: 2,
};

/** Cache serialized in JVM heap memory with 2x replication. */
export const MEMORY_ONLY_SER_2: StorageLevel = {
  useDisk: false,
  useMemory: true,
  useOffHeap: false,
  deserialized: false,
  replication: 2,
};

/** Cache deserialized in JVM heap memory, spilling to disk, with 2x replication. */
export const MEMORY_AND_DISK_2: StorageLevel = {
  useDisk: true,
  useMemory: true,
  useOffHeap: false,
  deserialized: true,
  replication: 2,
};

/** Cache serialized in JVM heap memory, spilling to disk, with 2x replication. */
export const MEMORY_AND_DISK_SER_2: StorageLevel = {
  useDisk: true,
  useMemory: true,
  useOffHeap: false,
  deserialized: false,
  replication: 2,
};

/** Cache on disk only with 2x replication. */
export const DISK_ONLY_2: StorageLevel = {
  useDisk: true,
  useMemory: false,
  useOffHeap: false,
  deserialized: false,
  replication: 2,
};

/** Cache serialized in off-heap memory. */
export const OFF_HEAP: StorageLevel = {
  useDisk: false,
  useMemory: true,
  useOffHeap: true,
  deserialized: false,
  replication: 1,
};
