# Parquet File Combiner

A  Python script to combine multiple Parquet files into a single file with schema validation, error recovery, and optional format conversion to CSV, JSON, or Feather.

## Features

 **Combine Parquet Files** - Merge multiple part files from Hadoop, Apache Spark, or AWS  
 **Schema Validation** - Automatically check for schema compatibility before combining  
 **Error Recovery** - Skip corrupt files and continue processing with `--skip-errors`  
 **File Filtering** - Use glob patterns to select specific files (e.g., `part-*.parquet`)  
 **Dry-Run Mode** - Preview what will happen without writing files  
 **Detailed Logging** - Track all operations in `combine_parquet.log`  
 **Format Conversion** - Convert combined result to CSV, JSON, or Feather  
 **Safety Checks** - Prevents output file from being placed in same directory as input  
 **Progress Tracking** - See file list with sizes before processing  
 **Comprehensive Summaries** - Get detailed stats about combined output  

## Requirements

```bash
conda create --name parquet_env -y
conda activate parquet_env
conda install -c conda-forge pyarrow -y
python -c "import pyarrow; print('PyArrow version:', pyarrow.__version__)"
```

## Usage

### Basic Usage (Interactive Mode)

```bash
python combine-parquet.py
```

Uses default paths configured in the script and prompts for format conversion.

### Command-Line Options

```bash
python combine-parquet.py --input <folder> --output <file.parquet> [options]
```

#### Arguments

| Argument | Short | Description | Default |
|----------|-------|-------------|---------|
| `--input` | `-i` | Input folder containing Parquet files | Configured default |
| `--output` | `-o` | Output file path for combined Parquet | Configured default |
| `--pattern` | `-p` | File pattern to match (glob) | `*.parquet` |
| `--dry-run` | `-d` | Preview without writing files | Disabled |
| `--skip-errors` | `-s` | Skip corrupt files and continue | Disabled |
| `--no-convert` | - | Skip format conversion prompt | Disabled |
| `--validate-only` | `-v` | Only validate files without combining | Disabled |
| `--help` | `-h` | Show help message | - |

## Examples

### Example 1: Basic Combination
```bash
python combine-parquet.py --input "./data" --output "combined.parquet"
```

### Example 2: Dry-Run (Safe Preview)
```bash
python combine-parquet.py --input "./data" --output "combined.parquet" --dry-run
```

Shows file list, schema info, estimated size, and column count without writing anything.

### Example 3: Use File Pattern
```bash
python combine-parquet.py --input "./data" --output "combined.parquet" --pattern "part-*.parquet"
```

Only combines files matching the pattern `part-*.parquet`, ignoring others.

### Example 4: Skip Corrupt Files
```bash
python combine-parquet.py --input "./data" --output "combined.parquet" --skip-errors
```

If some files are corrupted, the script will log warnings, skip them, and continue with valid files.

### Example 5: Validate Only
```bash
python combine-parquet.py --input "./data" --output "combined.parquet" --validate-only
```

Only checks if all files can be read and schemas are compatible. Doesn't combine or write anything.

### Example 6: No Format Conversion Prompt
```bash
python combine-parquet.py --input "./data" --output "combined.parquet" --no-convert
```

Combines files but skips the interactive format conversion prompts.

## Safety Features

### 1. Output Directory Check
- **Error**: Output file cannot be in the same directory as input folder
- **Protection**: Prevents accidental overwriting of input files

### 2. Schema Validation
- **Check**: Validates schemas of sample files before combining
- **Action**: Logs warnings if schema mismatches are detected but attempts to combine anyway
- **Benefit**: Early detection of potential data issues

### 3. Error Recovery
- **Option**: Use `--skip-errors` to skip individual corrupt files
- **Logging**: All skipped files are recorded in `combine_parquet.log`
- **Benefit**: Partial success instead of complete failure

### 4. Path Validation
- **Check**: Ensures input folder exists and is a directory
- **Check**: Ensures at least one Parquet file is found
- **Benefit**: Fail fast with clear error messages

### 5. Comprehensive Logging
- **File**: All operations logged to `combine_parquet.log`
- **Console**: Real-time feedback to stdout
- **Content**: Timestamps, severity levels, detailed error messages

## Output

After successful combination, you'll see:

```
============================================================
COMBINATION COMPLETED SUCCESSFULLY
============================================================
Total files combined: 42
Output file size: 1234.56 MB
Total rows in combined file: 5,000,000
Total columns: 25
============================================================
```

## Logging

All operations are logged to `combine_parquet.log` in the script's directory.

## Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| Input folder does not exist | Path is wrong | Check the folder path |
| No Parquet files found | Wrong pattern or empty folder | Use correct glob pattern or check folder |
| Output same as input directory | Configuration error | Change output path to different folder |
| Schema mismatch | Files have different structures | Use `--skip-errors` or check data source |
| Memory error | Dataset too large | Consider processing in smaller batches |

## Troubleshooting

### Q: Script exits with "No Parquet files found"
**A:** Check your `--pattern` argument. Use `--pattern "*.parquet"` for all parquet files.

### Q: How do I check what will happen before actually combining?
**A:** Run with `--dry-run` flag to preview without writing.

### Q: Some files are corrupted. How do I skip them?
**A:** Use `--skip-errors` flag. Skipped files will be logged in `combine_parquet.log`.

### Q: How large can my input dataset be?
**A:** Limited by available RAM. For very large datasets, consider splitting or using chunked processing.

### Q: Can I combine files incrementally?
**A:** Not currently, but you can combine twice: first combine into intermediate file, then combine that with new files.