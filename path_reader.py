import sqlite3
import argparse
from pathlib import Path
from typing import List, Tuple
import json
from tabulate import tabulate

def analyze_database(db_path: str, display_limit: int = None) -> None:
    """
    Analyze the prime sums database and display statistics.
    
    Args:
        db_path: Path to the SQLite database
        display_limit: Optional limit on how many rows to display
    """
    try:
        with sqlite3.connect(db_path) as conn:
            # Query to get stats for each number
            query = """
            WITH RECURSIVE number_series AS (
                SELECT 0 as number
                UNION ALL
                SELECT number + 1
                FROM number_series
                WHERE number < 700
            )
            SELECT 
                ns.number,
                COALESCE(COUNT(p.number), 0) as path_count,
                COALESCE(MAX(json_array_length(p.path_list)), 0) as max_path_length,
                COALESCE(MIN(json_array_length(p.path_list)), 0) as min_path_length
            FROM number_series ns
            LEFT JOIN paths p ON ns.number = p.number
            GROUP BY ns.number
            ORDER BY ns.number;
            """
            
            cursor = conn.execute(query)
            results = cursor.fetchall()
            
            if not results:
                print("No results found in database.")
                return
            
            # Prepare table data
            headers = ['Number', 'Path Count', 'Max Path Length',  "Min Path Length"]
            
            # Apply limit if specified
            display_results = results
            if display_limit:
                display_results = results[:display_limit]
            
            # Print table
            print("\nPrime Sum Paths Analysis:")
            print(tabulate(display_results, headers=headers, tablefmt='grid'))
            
            # for result in results:
            #     print(result[3])
            
            if display_limit and len(results) > display_limit:
                print(f"\n(Showing first {display_limit} of {len(results)} entries)")
            
            # Calculate statistics
            total_numbers = len(results)
            total_paths = sum(row[1] for row in results)
            max_paths = max(results, key=lambda x: x[1])
            max_length = max(results, key=lambda x: x[2])
            avg_paths = total_paths / total_numbers if total_numbers > 0 else 0
            
            # Print summary statistics
            print("\nSummary Statistics:")
            print(f"Total numbers processed: {total_numbers:,}")
            print(f"Total unique paths found: {total_paths:,}")
            print(f"Average paths per number: {avg_paths:.2f}")
            print(f"Number with most paths: {max_paths[0]} ({max_paths[1]:,} paths)")
            print(f"Number with longest path: {max_length[0]} (length: {max_length[2]})")
            
            # Find numbers with no paths
            no_paths = [num for num, count, _ in results if count == 0]
            if no_paths:
                print(f"\nNumbers with no paths: {len(no_paths)}")
                if len(no_paths) <= 10:
                    print(f"List: {', '.join(map(str, no_paths))}")
                else:
                    print(f"First 10: {', '.join(map(str, no_paths[:10]))}")
            
            # Sample path display for a specific number
            if results:
                # Get example paths for the number with the most paths
                example_num = max_paths[0]
                cursor = conn.execute(
                    "SELECT path_list FROM paths WHERE number = ? LIMIT 10",
                    (example_num,)
                )
                example_paths = [json.loads(row[0]) for row in cursor.fetchall()]
                
                print(f"\nExample paths for number {example_num} (showing up to 10):")
                for i, path in enumerate(example_paths, 1):
                    print(f"Path {i}: {' + '.join(map(str, path))} = {sum(path)}")
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error analyzing results: {e}")

def main():
    dir = 'prime_sums_output'
    target = 2025
    limit = 1000
    
    
    db_path = Path(dir) / f"prime_sums_{target}.db"
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return
        
    analyze_database(str(db_path), limit)

if __name__ == '__main__':
    main()