#!/usr/bin/env python3

import argparse
import re

from compactiontask import CompactionTask

def parse_sstable_path(path:str) -> tuple:
    """
    Extract keyspace and table from an SSTable path.
    Assumes path format: /.../<keyspace>/<table>-<id>/<sstable_name>[:level]
    Returns a tuple of (keyspace, table).
    """
    parts = path.split('/')
    if len(parts) < 3:
        raise ValueError(f"Invalid SSTable path: {path} (too few components)")

    keyspace = parts[-3]  # e.g., 'keyspace1'
    table_dir = parts[-2]  # e.g., 'standard1-5f8909b1b77011ed83492d3530a51895'

    table_parts = table_dir.split('-')
    if not table_parts:
        raise ValueError(f"Invalid table directory format in path: {path}")
    
    return keyspace, table_parts[0]

def parse_sstable_name(path:str) -> tuple:
    """
    Extract SSTable name and level (optional) from an SSTable path.
    Returns a tuple of (sstable_name, level).
    """
    parts = path.split('/')
    if not parts:
        raise ValueError(f"Invalid SSTable path: {path} (empty after split)")

    match = re.search(r'^(.*?)(?::level=(\d+))?$', parts[-1])
    if match:
        sstable_name, level = match.groups()
        sstable_name = sstable_name.split("-Data")[0] if sstable_name else parts[-1]
        level = int(level) if level else None
    else:
        sstable_name = parts[-1].split("-Data")[0] if "-Data" in parts[-1] else parts[-1]
        level = None
 
    return sstable_name, level

def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Parse Cassandra compaction logs.')
    parser.add_argument('log_files', nargs='+', help='Paths to the log files')
    args = parser.parse_args()

    # Regular expressions for start and end log entries
    start_pattern = re.compile(
        r'^DEBUG \[.*?\] (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})  '
        r'CompactionTask.java:\d+ - Compacting \(([a-f0-9-]+)\)\s*\[(.*?)\]'
    )
    end_pattern = re.compile(
        r'^DEBUG \[.*?\] '
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})  CompactionTask.java:\d+ - Compacted \('
        r'(?P<task_id>[a-f0-9-]+)\)\s*\d+\s*sstables to \['
        r'(?P<sstable_path>.*?)\]\s+to level='
        r'(?P<sstable_level>\d+).\s+'
        r'(?P<original_size>[\d\.]+)'
        r'(?P<original_size_unit>\w+)\s+to\s+'
        r'(?P<compacted_size>[\d\.]+)'
        r'(?P<compacted_size_unit>\w+)\s+\(.*?\)\s+in\s+'
        r'(?P<compaction_delta_time>[\d,]+)'
        r'(?P<compaction_delta_time_unit>\w+)\.\s+Read Throughput = '
        r'(?P<read_throughput>[\d\.]+)'
        r'(?P<read_throughput_unit>\w+/s), Write Throughput = '
        r'(?P<write_throughput>[\d\.]+)'
        r'(?P<write_throughput_unit>\w+/s), Row Throughput = ~'
        r'(?P<row_throughput>[\d,]+)/'
        r'(?P<row_throughput_unit>\w)\.\s+(Partition Throughput = ~'
        r'(?P<partition_throughput>[\d,]+)/'
        r'(?P<partition_throughput_unit>\w)\.\s+)?'
        r'(?P<total_partitions_merged>[\d,]+)\D+'
        r'(?P<total_keys_written>[\d,]+)\.[\s\w]+'
        r'(?P<partition_merge_counts>{[\d\s:,]+})'
    )

    # List of CompactionTasks
    compactions: dict[str, CompactionTask] = {}

    # Process each log file
    for log_file in args.log_files:
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    # Check for start of compaction
                    start_match = start_pattern.search(line)
                    if start_match:
                        timestamp = start_match.group(1)  # e.g., '2024-12-01 14:17:52,836'
                        compaction_id = start_match.group(2)  # e.g., 'ab9dd040-afe6-11ef-ad7f-a9588d22a314'
                        sstable_list_str = start_match.group(3)  # List of input SSTables

                        # Parse input SSTables
                        input_sstables = sstable_list_str.split(', ')
                        
                        # Extract keyspace and table from the first SSTable (assumed consistent)
                        keyspace, table = parse_sstable_path(input_sstables[0])

                        # Check all SSTables are from the same keyspace and table.
                        # This should always be true!
                        for sstable in input_sstables[1:]:
                            k, t = parse_sstable_path(sstable)
                            if k != keyspace or t != table:
                                print(f"Warning: Mixed keyspace/table in compaction {compaction_id}")
                        
                        # Store SSTable names and levels
                        input_sstable_names = dict(parse_sstable_name(s) for s in input_sstables)

                        if compaction_id in compactions:
                            task = compactions[compaction_id]
                            task.start_time = timestamp
                            task.input_sstables = input_sstable_names
                            task.keyspace = keyspace
                            task.table = table
                        else:
                            task = CompactionTask(compaction_id)
                            task.start_time = timestamp
                            task.input_sstables = input_sstable_names
                            task.keyspace = keyspace
                            task.table = table
                            compactions[compaction_id] = task

                        continue

                    # Check for end of compaction
                    end_match = end_pattern.search(line)
                    if end_match:
                        timestamp = end_match.group("timestamp")  # e.g., '2024-12-01 14:17:55,468'
                        compaction_id = end_match.group("task_id")
                        output_sstable_str = end_match.group("sstable_path")
                        output_sstable_level = end_match.group("sstable_level")
                        original_size = float(end_match.group("original_size"))
                        original_size_unit = end_match.group("original_size_unit")
                        compacted_size = float(end_match.group("compacted_size"))
                        compacted_size_unit = end_match.group("compacted_size_unit")
                        compaction_delta_time = int(end_match.group("compaction_delta_time"))
                        compaction_delta_time_unit = end_match.group("compaction_delta_time_unit")
                        read_throughput = float(end_match.group("read_throughput"))
                        read_throughput_unit = end_match.group("read_throughput_unit")
                        write_throughput = float(end_match.group("write_throughput"))
                        write_throughput_unit = end_match.group("write_throughput_unit")
                        row_throughput = int(end_match.group("row_throughput"))
                        row_throughput_unit = end_match.group("row_throughput_unit")
                        # TODO: Handle case where there is no match for partition_throughput*
                        partition_throughput = int(end_match.group("partition_throughput"))
                        partition_throughput_unit = end_match.group("partition_throughput_unit")
                        total_partitions_merged = int(end_match.group("total_partitions_merged"))
                        total_keys_written = int(end_match.group("total_keys_written"))
                        partition_merge_counts = end_match.group("partition_merge_counts")


                        # Parse output SSTable, handling potential truncation or multiple outputs
                        output_sstables_path = output_sstable_str.split(', ')
                        if len(output_sstables_path) > 1:
                            raise ValueError(f"More than one output SSTable found: {output_sstable_str}")

                        keyspace, table = parse_sstable_path(output_sstables_path[0])
                        # TODO: Add {output_sstable_level} to dict. It is not present in path.
                        output_sstables = dict(parse_sstable_name(s) for s in output_sstables_path)

                        if compaction_id in compactions:
                            task = compactions[compaction_id]
                            task.end_time = timestamp
                            task.output_sstables = output_sstables
                            task.original_size = original_size
                            task.original_size_unit = original_size_unit
                            task.compacted_size = compacted_size
                            task.compacted_size_unit = compacted_size_unit
                            task.compaction_delta_time = compaction_delta_time
                            task.compaction_delta_time_unit = compaction_delta_time_unit
                            task.read_throughput = read_throughput
                            task.read_throughput_unit = read_throughput_unit
                            task.write_throughput = write_throughput
                            task.write_throughput_unit = write_throughput_unit
                            task.row_throughput = row_throughput
                            task.row_throughput_unit = row_throughput_unit
                            task.partition_throughput = partition_throughput
                            task.partition_throughput_unit = partition_throughput_unit
                            task.total_partitions_merged = total_partitions_merged
                            task.total_keys_written = total_keys_written
                            task.partition_merge_counts = partition_merge_counts
                        else:
                            task = CompactionTask(compaction_id)
                            task.end_time = timestamp
                            task.output_sstables = output_sstables
                            task.original_size = original_size
                            task.original_size_unit = original_size_unit
                            task.compacted_size = compacted_size
                            task.compacted_size_unit = compacted_size_unit
                            task.compaction_delta_time = compaction_delta_time
                            task.compaction_delta_time_unit = compaction_delta_time_unit
                            task.read_throughput = read_throughput
                            task.read_throughput_unit = read_throughput_unit
                            task.write_throughput = write_throughput
                            task.write_throughput_unit = write_throughput_unit
                            task.row_throughput = row_throughput
                            task.row_throughput_unit = row_throughput_unit
                            task.partition_throughput = partition_throughput
                            task.partition_throughput_unit = partition_throughput_unit
                            task.total_partitions_merged = total_partitions_merged
                            task.total_keys_written = total_keys_written
                            task.partition_merge_counts = partition_merge_counts
        except FileNotFoundError:
            print(f"Error: Log file {log_file} not found")
            continue

# TODO: Add print or test to validate parsing is correct.

if __name__ == '__main__':
    main()
