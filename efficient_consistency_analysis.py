#!/usr/bin/env python3
"""
Efficient Product Consistency Analysis for Large Datasets
Processes data in chunks to handle large files without memory issues
"""

import pandas as pd
import os
from collections import defaultdict, Counter

def analyze_large_dataset_efficiently():
    """
    Analyze the large dataset efficiently using chunked processing
    """
    file_path = "/Users/kaner/Downloads/traderjoes-dump-3.csv"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"📁 Analyzing large dataset: {file_path}")
    print(f"📊 File size: {file_size_mb:.2f} MB")
    
    # Step 1: Analyze data structure and find stores
    print("\n🔍 STEP 1: Analyzing data structure...")
    stores = set()
    total_rows = 0
    
    # Read in chunks to find all stores
    chunk_size = 100000  # Process 100k rows at a time
    for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
        total_rows += len(chunk)
        stores.update(chunk['store_code'].astype(str).unique())
        
        if chunk_num % 10 == 0:
            print(f"  Processed {total_rows:,} rows...")
    
    # Clean store codes (remove non-numeric)
    valid_stores = {code for code in stores if code.isdigit()}
    print(f"✅ Found {len(valid_stores)} valid stores: {sorted(valid_stores)}")
    print(f"✅ Total rows: {total_rows:,}")
    
    # Step 2: Find items that appear in all stores
    print("\n🎯 STEP 2: Finding items in all stores...")
    
    # Count stores per item across all chunks
    item_store_counts = Counter()
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Clean store codes
        chunk['store_code'] = chunk['store_code'].astype(str)
        chunk = chunk[chunk['store_code'].isin(valid_stores)]
        
        # Count unique stores per item
        for item, stores_in_chunk in chunk.groupby('item_title')['store_code'].unique().items():
            item_store_counts[item] = max(item_store_counts[item], len(stores_in_chunk))
    
    # Find items in all stores
    items_in_all_stores = {item: count for item, count in item_store_counts.items() 
                          if count == len(valid_stores)}
    
    print(f"✅ Found {len(items_in_all_stores)} items that appear in ALL stores")
    
    if len(items_in_all_stores) > 0:
        print("\nTop 10 items in all stores:")
        for i, (item, count) in enumerate(list(items_in_all_stores.items())[:10], 1):
            print(f"{i:2d}. {item}")
    
    # Step 3: Filter by recency and duration
    print("\n📅 STEP 3: Filtering by recency and duration...")
    
    # Find time range
    print("  Finding time range...")
    min_time = None
    max_time = None
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Clean the data first - remove any rows where inserted_at is not a valid date
        chunk = chunk[chunk['inserted_at'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}')]
        
        if len(chunk) > 0:
            chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
            # Remove any NaT values
            chunk = chunk.dropna(subset=['inserted_at'])
            
            if len(chunk) > 0:
                chunk_min = chunk['inserted_at'].min()
                chunk_max = chunk['inserted_at'].max()
                
                if min_time is None or chunk_min < min_time:
                    min_time = chunk_min
                if max_time is None or chunk_max > max_time:
                    max_time = chunk_max
    
    if min_time is None or max_time is None:
        print("  ❌ Could not determine time range - skipping time-based filtering")
        print("  This might indicate data quality issues with timestamps")
        return None
    
    print(f"  Data spans: {min_time} to {max_time}")
    time_span_days = (max_time - min_time).days
    print(f"  Total span: {time_span_days} days ({time_span_days/365:.1f} years)")
    
    # Find recent items (within last day)
    recent_cutoff = max_time - pd.Timedelta(days=1)
    print(f"  Recent cutoff: {recent_cutoff}")
    
    recent_items = set()
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Clean the data first
        chunk = chunk[chunk['inserted_at'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}')]
        
        if len(chunk) > 0:
            chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
            chunk = chunk.dropna(subset=['inserted_at'])
            
            if len(chunk) > 0:
                recent_chunk = chunk[chunk['inserted_at'] >= recent_cutoff]
                recent_items.update(recent_chunk['item_title'].unique())
    
    print(f"  Items in recent query: {len(recent_items):,}")
    
    # Intersect with items in all stores
    recent_items_in_all_stores = recent_items.intersection(set(items_in_all_stores.keys()))
    print(f"  Items in all stores AND recent: {len(recent_items_in_all_stores):,}")
    
    # Step 4: Check duration for filtered items
    print("\n⏰ STEP 4: Checking duration (1+ year requirement)...")
    
    items_with_duration = []
    
    for item in list(recent_items_in_all_stores)[:100]:  # Check first 100 items
        print(f"  Checking: {item[:40]}...")
        
        first_seen = None
        last_seen = None
        
        # Find first and last appearance
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Clean the data first
            chunk = chunk[chunk['inserted_at'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}')]
            
            if len(chunk) > 0:
                chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
                chunk = chunk.dropna(subset=['inserted_at'])
                
                if len(chunk) > 0:
                    item_chunk = chunk[chunk['item_title'] == item]
                    
                    if len(item_chunk) > 0:
                        chunk_min = item_chunk['inserted_at'].min()
                        chunk_max = item_chunk['inserted_at'].max()
                        
                        if first_seen is None or chunk_min < first_seen:
                            first_seen = chunk_min
                        if last_seen is None or chunk_max > last_seen:
                            last_seen = chunk_max
        
        if first_seen and last_seen:
            duration_days = (last_seen - first_seen).days
            if duration_days >= 365:
                items_with_duration.append({
                    'item': item,
                    'first_seen': first_seen,
                    'last_seen': last_seen,
                    'duration_days': duration_days,
                    'duration_years': duration_days / 365
                })
                print(f"    ✅ {duration_days} days ({duration_days/365:.1f} years)")
            else:
                print(f"    ❌ {duration_days} days")
        else:
            print(f"    ❌ Could not determine duration")
    
    # Sort by duration
    items_with_duration.sort(key=lambda x: x['duration_days'], reverse=True)
    
    print(f"\n🎉 Found {len(items_with_duration)} items meeting all criteria!")
    
    # Final summary
    print("\n📊 FINAL SUMMARY")
    print("=" * 60)
    print(f"Total rows analyzed: {total_rows:,}")
    print(f"Valid stores: {len(valid_stores)}")
    print(f"Items in all stores: {len(items_in_all_stores):,}")
    print(f"Items meeting all criteria: {len(items_with_duration):,}")
    print(f"  - In all stores")
    print(f"  - In most recent data query")
    print(f"  - Present for 1+ year")
    
    if items_with_duration:
        print(f"\nTop 10 longest-running core products:")
        print("-" * 60)
        for i, item_info in enumerate(items_with_duration[:10], 1):
            print(f"{i:2d}. {item_info['item'][:50]}...")
            print(f"    Duration: {item_info['duration_years']:.1f} years ({item_info['duration_days']} days)")
            print(f"    First seen: {item_info['first_seen'].strftime('%Y-%m-%d')}")
            print(f"    Last seen: {item_info['last_seen'].strftime('%Y-%m-%d')}")
            print()
    
    return items_with_duration

def main():
    """Main function"""
    print("Trader Joe's Efficient Product Consistency Analysis")
    print("=" * 70)
    
    # Run the efficient analysis
    results = analyze_large_dataset_efficiently()
    
    if results:
        print(f"\n✅ Analysis complete! Found {len(results)} core products.")
    else:
        print("\n❌ Analysis failed or no results found.")

if __name__ == "__main__":
    main() 