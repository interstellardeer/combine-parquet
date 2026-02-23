import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import csv, json, feather
import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import warnings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('combine_parquet.log'),
        logging.StreamHandler()
    ]
)

def validate_paths(input_folder: str, output_file: str) -> None:
    """Validate input and output paths for errors."""
    if not os.path.exists(input_folder):
        raise FileNotFoundError(f"Input folder does not exist: {input_folder}")
    
    if not os.path.isdir(input_folder):
        raise NotADirectoryError(f"Input path is not a directory: {input_folder}")
    
    output_dir = os.path.dirname(output_file)
    if os.path.abspath(output_dir) == os.path.abspath(input_folder):
        raise ValueError("Output file cannot be placed in the same directory as the input folder to avoid overwriting input files.")
    
    logging.info(f"Input folder: {input_folder}")
    logging.info(f"Output file: {output_file}")


def find_parquet_files(input_folder: str, pattern: str = "*.parquet") -> List[str]:
    """Find parquet files matching the pattern in the input folder."""
    from fnmatch import fnmatch
    
    files = []
    try:
        for f in os.listdir(input_folder):
            full_path = os.path.join(input_folder, f)
            if os.path.isfile(full_path) and fnmatch(f, pattern):
                files.append(f)
        
        if not files:
            raise ValueError(f"No files matching pattern '{pattern}' found in: {input_folder}")
        
        files.sort()
        return files
    except Exception as e:
        logging.error(f"Error finding parquet files: {e}")
        raise


def validate_schemas(input_folder: str, parquet_files: List[str]) -> Optional[Dict]:
    """Validate that all parquet files have compatible schemas."""
    try:
        schemas = {}
        schema_warnings = []
        
        for file in parquet_files[:min(3, len(parquet_files))]:  # Check first 3 files for efficiency
            file_path = os.path.join(input_folder, file)
            try:
                schema = pq.read_schema(file_path)
                schemas[file] = schema
            except Exception as e:
                logging.warning(f"Could not read schema from {file}: {e}")
                schema_warnings.append(file)
        
        if not schemas:
            raise ValueError("Could not read schema from any parquet files")
        
        # Compare schemas
        first_schema = next(iter(schemas.values()))
        for file, schema in schemas.items():
            if schema != first_schema:
                logging.warning(f"Schema mismatch detected in {file}")
                schema_warnings.append(file)
        
        if schema_warnings:
            logging.warning(f"Schema warnings found in files: {schema_warnings}. Attempting to combine anyway...")
        
        return first_schema
    except Exception as e:
        logging.error(f"Schema validation failed: {e}")
        raise


def print_file_summary(parquet_files: List[str], input_folder: str) -> None:
    """Print summary of files to be combined."""
    logging.info(f"\n{'='*60}")
    logging.info(f"Total Parquet files to combine: {len(parquet_files)}")
    logging.info(f"{'='*60}")
    logging.info("Files to be combined:")
    
    for i, file in enumerate(parquet_files, 1):
        file_path = os.path.join(input_folder, file)
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
            logging.info(f"  {i:3d}. {file:<50s} ({file_size:>8.2f} MB)")
        except Exception as e:
            logging.info(f"  {i:3d}. {file:<50s} (size unknown)")
    
    logging.info(f"{'='*60}\n")


def calculate_output_size_estimate(input_folder: str, parquet_files: List[str]) -> float:
    """Estimate total output size in MB."""
    total_size = 0
    for file in parquet_files:
        file_path = os.path.join(input_folder, file)
        try:
            total_size += os.path.getsize(file_path)
        except Exception:
            pass
    
    return total_size / (1024 * 1024)  # Convert to MB


def perform_dry_run(input_folder: str, parquet_files: List[str], 
                   output_file: str) -> None:
    """Perform a dry run showing what would be done."""
    logging.info("\n" + "="*60)
    logging.info("DRY-RUN MODE (No files will be written)")
    logging.info("="*60)
    
    try:
        schema = validate_schemas(input_folder, parquet_files)
        print_file_summary(parquet_files, input_folder)
        
        estimated_size = calculate_output_size_estimate(input_folder, parquet_files)
        logging.info(f"Estimated combined file size: {estimated_size:.2f} MB")
        logging.info(f"Schema columns: {len(schema)}")
        logging.info(f"Column names: {schema.names}")
        logging.info(f"Output file would be saved to: {output_file}")
        logging.info("="*60 + "\n")
    except Exception as e:
        logging.error(f"Dry-run failed: {e}")
        raise


def combine_parquet_files(input_folder: str, output_file: str, 
                         parquet_files: List[str], skip_errors: bool = False) -> Tuple[bool, int]:
    """Combine parquet files into a single file."""
    try:
        logging.info("Starting to combine Parquet files...")
        
        # Create output directory
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            logging.info(f"Created output directory: {output_dir}")
        
        if skip_errors:
            # Process files individually with error handling
            tables = []
            failed_files = []
            
            for file in parquet_files:
                try:
                    file_path = os.path.join(input_folder, file)
                    table = pq.read_table(file_path)
                    tables.append(table)
                except Exception as e:
                    logging.warning(f"Skipped file {file} due to error: {e}")
                    failed_files.append(file)
            
            if not tables:
                raise ValueError("No files could be read successfully")
            
            if failed_files:
                logging.warning(f"Failed to process {len(failed_files)} file(s): {failed_files}")
            
            import pyarrow as pa
            table = pa.concat_tables(tables)
        else:
            # Use dataset API for standard processing
            dataset = ds.dataset(input_folder, format="parquet")
            table = dataset.to_table()
        
        # Write combined file
        pq.write_table(table, output_file)
        
        logging.info(f"Successfully combined Parquet files")
        logging.info(f"Output file: {output_file}")
        
        return True, len(parquet_files)
    except Exception as e:
        logging.error(f"Failed to combine files: {e}")
        raise


def print_final_summary(output_file: str, num_files: int, table_info: Dict = None) -> None:
    """Print final summary after successful combination."""
    logging.info("\n" + "="*60)
    logging.info("COMBINATION COMPLETED SUCCESSFULLY")
    logging.info("="*60)
    logging.info(f"Total files combined: {num_files}")
    
    try:
        if os.path.exists(output_file):
            output_size = os.path.getsize(output_file) / (1024 * 1024)
            logging.info(f"Output file size: {output_size:.2f} MB")
            
            table = pq.read_table(output_file)
            logging.info(f"Total rows in combined file: {table.num_rows:,}")
            logging.info(f"Total columns: {table.num_columns}")
    except Exception as e:
        logging.warning(f"Could not retrieve output file info: {e}")
    
    logging.info("="*60 + "\n")


def handle_format_conversion(output_file: str, table) -> None:
    """Handle optional format conversion."""
    try:
        convert = input("Do you want to convert to another format? (y/n): ").lower().strip()
        if convert in ['y', 'yes']:
            format_choice = input("Choose format (csv, json, feather): ").lower().strip()
            
            try:
                if format_choice == 'csv':
                    output_file_csv = output_file.replace('.parquet', '.csv')
                    csv.write_csv(table, output_file_csv)
                    logging.info(f"Converted to CSV: {output_file_csv}")
                elif format_choice == 'json':
                    output_file_json = output_file.replace('.parquet', '.json')
                    json.write_json(table, output_file_json)
                    logging.info(f"Converted to JSON: {output_file_json}")
                elif format_choice == 'feather':
                    output_file_feather = output_file.replace('.parquet', '.feather')
                    feather.write_feather(table, output_file_feather)
                    logging.info(f"Converted to Feather: {output_file_feather}")
                else:
                    logging.error("Invalid format choice. Supported formats: csv, json, feather")
            except Exception as e:
                logging.error(f"Error during conversion: {e}")
    except Exception as e:
        logging.error(f"Error in format conversion: {e}")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Combine multiple Parquet files into a single file with optional format conversion.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python combine-parquet.py --input "./data" --output "combined.parquet"
  python combine-parquet.py --input "./data" --output "combined.parquet" --dry-run
  python combine-parquet.py --input "./data" --output "combined.parquet" --pattern "part-*.parquet" --skip-errors
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        help='Input folder containing Parquet files'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        required=True,
        help='Output file path for combined Parquet file'
    )
    
    parser.add_argument(
        '--pattern', '-p',
        type=str,
        default='*.parquet',
        help='File pattern to match (default: *.parquet). Example: part-*.parquet'
    )
    
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Perform a dry run without writing files'
    )
    
    parser.add_argument(
        '--skip-errors', '-s',
        action='store_true',
        help='Skip files that cause errors and continue processing'
    )
    
    parser.add_argument(
        '--no-convert',
        action='store_true',
        help='Skip the format conversion prompt'
    )
    
    parser.add_argument(
        '--validate-only', '-v',
        action='store_true',
        help='Only validate files without combining'
    )
    
    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()
    
    try:
        logging.info("Starting Parquet Combine Process...")
        logging.info(f"Python version: {sys.version}")
        
        # Validate paths
        validate_paths(args.input, args.output)
        
        # Find parquet files
        logging.info(f"Searching for files matching pattern: {args.pattern}")
        parquet_files = find_parquet_files(args.input, args.pattern)
        
        # Print file summary
        print_file_summary(parquet_files, args.input)
        
        # Validate schemas
        logging.info("Validating file schemas...")
        schema = validate_schemas(args.input, parquet_files)
        logging.info(f"Schema validation completed. Found {len(schema)} columns.")
        
        # Handle dry-run mode
        if args.dry_run:
            perform_dry_run(args.input, parquet_files, args.output)
            logging.info("Dry-run completed. Use without --dry-run to perform actual combination.")
            return
        
        # Handle validate-only mode
        if args.validate_only:
            logging.info("Validation completed successfully. Ready to combine.")
            return
        
        # Combine files
        success, num_files = combine_parquet_files(
            args.input, 
            args.output, 
            parquet_files,
            skip_errors=args.skip_errors
        )
        
        if success:
            # Print final summary
            table = pq.read_table(args.output)
            print_final_summary(args.output, num_files)
            
            # Handle format conversion
            if not args.no_convert:
                handle_format_conversion(args.output, table)
            
            logging.info("Process completed successfully!")
        
    except FileNotFoundError as e:
        logging.error(f"File not found error: {e}")
        sys.exit(1)
    except NotADirectoryError as e:
        logging.error(f"Directory error: {e}")
        sys.exit(1)
    except ValueError as e:
        logging.error(f"Value error: {e}")
        sys.exit(1)
    except OSError as e:
        logging.error(f"OS error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Critical error in main execution: {e}", exc_info=True)
        sys.exit(1)