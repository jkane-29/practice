#!/usr/bin/env python3
"""
Large CSV Data Analyzer for Trader Joe's Data
Handles large CSV files efficiently using multiple approaches
"""

import pandas as pd
import dask.dataframe as dd
import vaex
import os
import sys
from pathlib import Path

class LargeCSVAnalyzer:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.file_size = os.path.getsize(csv_path) / (1024 * 1024)  # Size in MB
        
    def get_file_info(self):
        """Get basic information about the CSV file"""
        print(f"📁 File: {self.csv_path}")
        print(f"📏 Size: {self.file_size:.2f} MB")
        
        # Count lines efficiently
        with open(self.csv_path, 'r') as f:
            line_count = sum(1 for _ in f)
        print(f"📊 Total lines: {line_count:,}")
        
        # Preview first few lines
        print("\n🔍 First 5 lines preview:")
        with open(self.csv_path, 'r') as f:
            for i, line in enumerate(f):
                if i < 5:
                    print(f"Line {i+1}: {line.strip()}")
                else:
                    break
    
    def analyze_with_pandas_chunks(self, chunk_size=10000):
        """Analyze CSV in chunks using pandas (good for medium-large files)"""
        print(f"\n🐼 Analyzing with pandas (chunk size: {chunk_size:,})")
        
        # Read in chunks and analyze
        chunk_list = []
        total_rows = 0
        
        for chunk_num, chunk in enumerate(pd.read_csv(self.csv_path, chunksize=chunk_size)):
            total_rows += len(chunk)
            chunk_list.append(chunk)
            
            if chunk_num < 3:  # Show first 3 chunks
                print(f"Chunk {chunk_num + 1}: {len(chunk):,} rows, {len(chunk.columns)} columns")
                print(f"Columns: {list(chunk.columns)}")
                print(f"Sample data:\n{chunk.head(2)}\n")
            
            if chunk_num >= 9:  # Limit to first 10 chunks for demo
                print(f"... and {len(chunk_list) - 10} more chunks")
                break
        
        print(f"📈 Total rows processed: {total_rows:,}")
        return chunk_list
    
    def analyze_with_dask(self):
        """Analyze CSV using Dask (excellent for very large files)"""
        print(f"\n⚡ Analyzing with Dask")
        
        try:
            # Read CSV with Dask
            ddf = dd.read_csv(self.csv_path)
            
            print(f"📊 Dask DataFrame shape: {ddf.shape.compute()}")
            print(f"🔤 Columns: {list(ddf.columns)}")
            print(f"📋 Data types:\n{ddf.dtypes}")
            
            # Show sample data
            print(f"\n📄 Sample data (first 5 rows):")
            print(ddf.head())
            
            return ddf
            
        except Exception as e:
            print(f"❌ Error with Dask: {e}")
            return None
    
    def analyze_with_vaex(self):
        """Analyze CSV using Vaex (very fast for large files)"""
        print(f"\n🚀 Analyzing with Vaex")
        
        try:
            # Read CSV with Vaex
            df = vaex.read_csv(self.csv_path)
            
            print(f"📊 Vaex DataFrame shape: {df.shape}")
            print(f"🔤 Columns: {list(df.columns)}")
            print(f"📋 Data types:\n{df.dtypes}")
            
            # Show sample data
            print(f"\n📄 Sample data (first 5 rows):")
            print(df.head())
            
            return df
            
        except Exception as e:
            print(f"❌ Error with Vaex: {e}")
            return None
    
    def basic_operations_demo(self, method='dask'):
        """Demonstrate basic operations on the data"""
        print(f"\n🔧 Basic Operations Demo using {method.upper()}")
        
        if method == 'dask':
            df = self.analyze_with_dask()
            if df is None:
                return
            
            # Basic operations
            print("\n📊 Basic statistics:")
            print(df.describe().compute())
            
            # Column operations
            print(f"\n🔍 Unique values in first column:")
            first_col = df.columns[0]
            print(df[first_col].value_counts().head(10).compute())
            
        elif method == 'vaex':
            df = self.analyze_with_vaex()
            if df is None:
                return
            
            # Basic operations
            print("\n📊 Basic statistics:")
            print(df.describe())
            
            # Column operations
            print(f"\n🔍 Unique values in first column:")
            first_col = df.columns[0]
            print(df[first_col].value_counts().head(10))
    
    def filter_and_sample(self, method='dask', sample_size=1000):
        """Filter data and create a smaller sample for detailed analysis"""
        print(f"\n🔍 Creating sample dataset ({sample_size:,} rows) using {method.upper()}")
        
        if method == 'dask':
            df = dd.read_csv(self.csv_path)
            # Take a sample
            sample = df.sample(frac=sample_size/df.shape[0].compute(), random_state=42)
            sample_df = sample.compute()
            
        elif method == 'vaex':
            df = vaex.read_csv(self.csv_path)
            # Take a sample
            sample_df = df.sample(n=sample_size)
        
        print(f"📊 Sample shape: {sample_df.shape}")
        print(f"💾 Sample size: {sample_df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
        # Save sample for further analysis
        sample_path = f"trader_joes_sample_{sample_size}.csv"
        sample_df.to_csv(sample_path, index=False)
        print(f"💾 Sample saved to: {sample_path}")
        
        return sample_df

def main():
    """Main function to run the analysis"""
    print("🛒 Trader Joe's Large CSV Analyzer")
    print("=" * 50)
    
    # Check if CSV file path is provided
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        # Ask user for CSV file path
        csv_path = input("Enter the path to your Trader Joe's CSV file: ").strip()
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return
    
    # Initialize analyzer
    analyzer = LargeCSVAnalyzer(csv_path)
    
    # Get basic file info
    analyzer.get_file_info()
    
    # Choose analysis method based on file size
    if analyzer.file_size < 100:  # Less than 100 MB
        print("\n📁 File is relatively small, using pandas chunks...")
        analyzer.analyze_with_pandas_chunks()
    elif analyzer.file_size < 1000:  # Less than 1 GB
        print("\n📁 File is medium-sized, using Dask...")
        analyzer.analyze_with_dask()
        analyzer.basic_operations_demo('dask')
    else:  # Very large file
        print("\n📁 File is very large, using Vaex...")
        analyzer.analyze_with_vaex()
        analyzer.basic_operations_demo('vaex')
    
    # Create sample dataset
    print("\n" + "=" * 50)
    analyzer.filter_and_sample(method='dask' if analyzer.file_size < 1000 else 'vaex')
    
    print("\n✅ Analysis complete! Check the generated sample file for further exploration.")

if __name__ == "__main__":
    main() 