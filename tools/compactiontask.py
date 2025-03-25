class CompactionTask:
    # Unique identifier for the compaction task.
    task_id: str
    # Timestamp when the compaction started.
    start_time: str
    # Timestamp when the compaction ended.
    end_time: str
    # The keyspace associated with the SSTables.
    keyspace: str
    # The table associated with the SSTables.
    table: str
    # Dict of SSTable names and their level used as input.
    input_sstables: dict
    # Dict of SSTable names (usually only one) and their level produced as output
    output_sstables: dict
    # The size of the input SSTables.
    original_size: float
    # The size unit, e.g., MiB.
    original_size_unit: str
    # The size of the output SSTable.
    compacted_size: float
    # The size unit, e.g., MiB.
    compacted_size_unit: str
    # The unit of time the compaction task took to complete.
    compaction_delta_time: int
    # The unit of time, e.g., ms.
    compaction_delta_time_unit: str = "ms"
    # The I/O read throughput of the input SSTables.
    read_throughput: float
    # The throughput unit, e.g., MiB/s.
    read_throughput_unit: str
    # The I/O write throughput of the output SSTable.
    write_throughput: float
    # The throughput unit, e.g., MiB/s.
    write_throughput_unit: str
    # The approximate row throughput.
    row_throughput: int
    # The throughput unit, e.g., s.
    row_throughput_unit: str = "second"
    # The approximate partition throughput.
    partition_throughput: int
    # The throughput unit, e.g., s.
    partition_throughput_unit: str = "second"
    # The number of partitions merged from input SSTables.
    total_partitions_merged: int
    # The number of partition keys written to the output SSTable.
    total_keys_written: int
    # The number of partition keys written to the output SSTable.
    # No use for these at the moment.
    partition_merge_counts: str
    # List of UUIDs of child compaction tasks.
    children: list = []


    def __init__(self, task_id):
        """
        Initialize a CompactionTask object with compaction details.

        Args:
            task_id (str): Unique identifier for the compaction task.
        """
        self.task_id = task_id
