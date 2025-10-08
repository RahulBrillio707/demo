#!/usr/bin/env python3
"""
Enhanced batch processing with better error handling, rate limiting, and resource management
for processing large numbers of SAS files in parallel.
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading
from typing import List, Dict, Any
import queue
import traceback

# Import the conversion workflow
from conversion_workflow import pipeline as conversion_pipeline, SASPipelineState

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedBatchProcessor:
    """Enhanced batch processor with rate limiting, memory management, and better error handling"""
    
    def __init__(self, max_workers=4, rate_limit_delay=0.5, max_retries=3):
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay  # Delay between LLM requests
        self.max_retries = max_retries
        self.processing_lock = threading.Lock()
        self.request_semaphore = threading.Semaphore(max_workers * 2)  # Limit concurrent LLM requests
        self.results_queue = queue.Queue()
        
    def process_single_file_enhanced(self, file_path: str, output_dir: str, attempt: int = 1) -> Dict[str, Any]:
        """Process a single SAS file with enhanced error handling and rate limiting"""
        thread_name = threading.current_thread().name
        thread_id = threading.current_thread().ident
        
        logger.info(f"[{thread_name}] Starting processing: {os.path.basename(file_path)} (attempt {attempt})")
        
        try:
            start_time = time.time()
            
            # Rate limiting: Acquire semaphore to limit concurrent LLM requests
            with self.request_semaphore:
                logger.debug(f"Thread {thread_name} attempting to acquire semaphore for file: {file_path}")
                with self.request_semaphore:
                    logger.debug(f"Semaphore acquired for file: {file_path}")
                # Add delay to prevent overwhelming the LLM API
                if attempt > 1:
                    delay = self.rate_limit_delay * attempt
                    logger.info(f"[{thread_name}] Retry delay: {delay}s")
                    time.sleep(delay)
                
                # Read the SAS file
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                        script_content = f.read()
                except UnicodeDecodeError:
                    # Fallback to latin-1 encoding for problematic files
                    with open(file_path, 'r', encoding='latin-1') as f:
                        script_content = f.read()
                
                # Validate file content
                if not script_content.strip():
                    raise ValueError("Empty or whitespace-only file")
                
                if len(script_content) > 100000:  # 100KB limit
                    logger.warning(f"[{thread_name}] Large file detected: {len(script_content)} chars")
                
                # Create state with unique identifiers
                # Use smaller chunk size for better processing of large files
                chunk_size = 100
                init_state = SASPipelineState(
                    file_path=file_path,
                    script_content=script_content,
                    chunk_size=chunk_size
                )
                
                logger.debug(f"[{thread_name}] Using chunk_size: {chunk_size} for {len(script_content.splitlines())} lines")
                
                # Generate unique thread config to prevent conflicts
                unique_thread_id = f"batch_{thread_id}_{int(time.time()*1000000)}_{attempt}"
                config = {"configurable": {"thread_id": unique_thread_id}}
                
                logger.debug(f"[{thread_name}] Using thread_id: {unique_thread_id}")
                
                # Process with conversion workflow
                raw_state = conversion_pipeline.invoke(init_state, config=config)
                
                # Validate the result
                if not isinstance(raw_state, dict):
                    raise ValueError(f"Invalid pipeline result type: {type(raw_state)}")
                
                final_state = SASPipelineState(**raw_state)
                
                # Check if conversion was successful
                if not hasattr(final_state, 'merged_pyspark_code') or not final_state.merged_pyspark_code.strip():
                    raise ValueError("No PySpark code generated")
                
                # Create output filename
                input_filename = Path(file_path).stem
                output_filename = f"{input_filename}_converted.py"
                output_path = os.path.join(output_dir, output_filename)
                
                # Ensure output directory exists
                os.makedirs(output_dir, exist_ok=True)
                
                # Save the converted PySpark code
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(final_state.merged_pyspark_code)
                
                duration = time.time() - start_time
                
                result = {
                    'input_file': file_path,
                    'output_file': output_path,
                    'status': 'success',
                    'duration': duration,
                    'attempt': attempt,
                    'thread_name': thread_name,
                    'thread_id': unique_thread_id,
                    'python_valid': getattr(final_state, 'python_validation', {}).get('syntax_valid', False),
                    'pyspark_valid': getattr(final_state, 'pyspark_validation', {}).get('basic_syntax_valid', False),
                    'chunks_processed': getattr(final_state, 'total_chunks_processed', 0),
                    'file_size': len(script_content),
                    'error': None
                }
                
                logger.info(f"[{thread_name}] SUCCESS: {os.path.basename(file_path)} in {duration:.2f}s")
                return result
                
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            error_msg = str(e)
            full_traceback = traceback.format_exc()
            
            logger.error(f"[{thread_name}] ERROR: {os.path.basename(file_path)} - {error_msg}")
            
            # Retry logic for transient errors
            if attempt < self.max_retries and self._is_retryable_error(e):
                logger.info(f"[{thread_name}] Retrying {os.path.basename(file_path)} (attempt {attempt + 1})")
                time.sleep(self.rate_limit_delay * attempt)  # Exponential backoff
                return self.process_single_file_enhanced(file_path, output_dir, attempt + 1)
            
            result = {
                'input_file': file_path,
                'output_file': None,
                'status': 'error',
                'duration': duration,
                'attempt': attempt,
                'thread_name': thread_name,
                'error': error_msg,
                'full_traceback': full_traceback
            }
            
            return result
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is worth retrying"""
        error_str = str(error).lower()
        retryable_patterns = [
            'timeout',
            'connection',
            'rate limit',
            'temporary',
            'service unavailable',
            'internal server error',
            'bad gateway',
            'gateway timeout'
        ]
        return any(pattern in error_str for pattern in retryable_patterns)
    
    def run_batch_processing_enhanced(self, input_dir: str, output_dir: str) -> List[Dict[str, Any]]:
        """Run enhanced batch processing with better resource management"""
        logger.info(f"Starting enhanced batch processing: {input_dir} -> {output_dir}")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Find all SAS files
        sas_files = []
        for ext in ['*.sas', '*.SAS']:
            sas_files.extend(Path(input_dir).glob(ext))
        
        sas_files = [str(f) for f in sas_files]
        
        if not sas_files:
            logger.warning(f"No SAS files found in {input_dir}")
            return []
        
        logger.info(f"Found {len(sas_files)} SAS files to process")
        
        # Sort files by size (process smaller files first)
        sas_files_with_size = []
        for file_path in sas_files:
            try:
                size = os.path.getsize(file_path)
                sas_files_with_size.append((file_path, size))
            except OSError:
                sas_files_with_size.append((file_path, 0))
        
        sas_files_with_size.sort(key=lambda x: x[1])  # Sort by size
        sas_files = [f[0] for f in sas_files_with_size]
        
        results = []
        
        # Process files in parallel with controlled concurrency
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="BatchWorker") as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.process_single_file_enhanced, file_path, output_dir): file_path 
                for file_path in sas_files
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                completed += 1
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    status = "✅" if result['status'] == 'success' else "❌"
                    logger.info(f"[BATCH] {status} [{completed}/{len(sas_files)}] {os.path.basename(result['input_file'])}")
                    
                except Exception as e:
                    logger.error(f"[BATCH] Future execution failed for {os.path.basename(file_path)}: {str(e)}")
                    results.append({
                        'input_file': file_path,
                        'output_file': None,
                        'status': 'future_error',
                        'error': str(e),
                        'thread_name': 'unknown'
                    })
        
        # Summary
        successful = len([r for r in results if r['status'] == 'success'])
        failed = len([r for r in results if r['status'] != 'success'])
        
        logger.info(f"Batch processing completed: {successful} successful, {failed} failed")
        
        return results

def main():
    """Example usage of enhanced batch processing"""
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python enhanced_batch_processing.py <input_dir> <output_dir>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(input_dir):
        print(f"Error: Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    # Create enhanced processor
    processor = EnhancedBatchProcessor(
        max_workers=4,              # Reduce workers to prevent overwhelming
        rate_limit_delay=1.0,       # 1 second delay between requests
        max_retries=3               # Retry failed requests
    )
    
    # Run batch processing
    start_time = time.time()
    results = processor.run_batch_processing_enhanced(input_dir, output_dir)
    total_time = time.time() - start_time
    
    # Print summary
    successful = len([r for r in results if r['status'] == 'success'])
    failed = len([r for r in results if r['status'] != 'success'])
    
    print(f"\n{'='*60}")
    print(f"BATCH PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total Files: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Average Time per File: {total_time/len(results):.2f}s")
    
    if failed > 0:
        print(f"\nFailed Files:")
        for result in results:
            if result['status'] != 'success':
                print(f"  - {os.path.basename(result['input_file'])}: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()