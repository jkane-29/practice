#!/usr/bin/env python3
"""
Simple Product Consistency Analysis
Find items that appear in all stores and have been consistently available
"""

import pandas as pd
import os

def load_data():
    """Load the full Trader Joe's dataset"""
    file_path = "/Users/kaner/Downloads/traderjoes-dump-3.csv"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"📁 Loading full dataset: {file_path}")
    print(f"📊 File size: {file_size_mb:.2f} MB")
    
    try:
        # Load the full dataset with better data type handling
        print("🔄 Loading data (this may take a while for large files)...")
        df = pd.read_csv(file_path, low_memory=False)
        print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns")
        
        # Clean up data types
        print("🧹 Cleaning data types...")
        
        # Convert store_code to string to handle mixed types
        df['store_code'] = df['store_code'].astype(str)
        
        # Convert availability to numeric, handling any non-numeric values
        df['availability'] = pd.to_numeric(df['availability'], errors='coerce')
        
        # Convert retail_price to numeric
        df['retail_price'] = pd.to_numeric(df['retail_price'], errors='coerce')
        
        # Filter out invalid store codes (like 'store_code' header)
        print("🔍 Checking data quality...")
        print(f"Store codes before cleaning: {sorted(df['store_code'].unique())}")
        
        # Keep only numeric store codes
        valid_stores = [code for code in df['store_code'].unique() if code.isdigit()]
        df = df[df['store_code'].isin(valid_stores)]
        
        print(f"Store codes after cleaning: {sorted(valid_stores)}")
        print(f"Rows after cleaning: {len(df):,}")
        
        print("✅ Data types cleaned and validated")
        
        return df
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None

def step1_find_items_in_all_stores(df):
    """
    STEP 1: Find items that appear in ALL stores
    """
    print("\n🎯 STEP 1: Finding items in ALL stores")
    print("=" * 50)
    
    # Count how many stores each item appears in
    store_counts = df.groupby('item_title')['store_code'].nunique()
    
    # Get total number of stores
    total_stores = len(df['store_code'].unique())
    print(f"Total stores: {total_stores}")
    print(f"Store codes: {sorted(df['store_code'].unique())}")
    
    # Find items in ALL stores
    items_in_all_stores = store_counts[store_counts == total_stores]
    
    print(f"Found {len(items_in_all_stores)} items that appear in ALL stores")
    
    # Show some examples
    if len(items_in_all_stores) > 0:
        print("\nExamples:")
        for i, (item, count) in enumerate(items_in_all_stores.head(10).items(), 1):
            print(f"{i:2d}. {item}")
    
    return items_in_all_stores

def step2_filter_by_recency_and_duration(df, items_in_all_stores):
    """
    STEP 2: Filter items by recency (most recent query) and duration (at least 1 year)
    """
    print("\n📅 STEP 2: Filtering by recency and duration")
    print("=" * 50)
    
    # Convert timestamp to datetime
    df['inserted_at'] = pd.to_datetime(df['inserted_at'])
    
    # Find the most recent data query
    most_recent_time = df['inserted_at'].max()
    earliest_time = df['inserted_at'].min()
    
    print(f"Data spans from: {earliest_time} to {most_recent_time}")
    print(f"Most recent data query: {most_recent_time}")
    
    # Calculate the time span
    time_span = most_recent_time - earliest_time
    print(f"Total dataset time span: {time_span.days} days ({time_span.days/365:.1f} years)")
    
    # Filter for items that appear in the most recent query
    print(f"\n🔍 Finding items in the most recent data query...")
    
    # Get items from the most recent time period (within last day to handle multiple queries)
    recent_cutoff = most_recent_time - pd.Timedelta(days=1)
    recent_data = df[df['inserted_at'] >= recent_cutoff]
    
    recent_items = set(recent_data['item_title'].unique())
    print(f"Items in most recent query: {len(recent_items):,}")
    
    # Intersect with items that appear in all stores
    items_in_all_stores_set = set(items_in_all_stores.index)
    recent_items_in_all_stores = recent_items.intersection(items_in_all_stores_set)
    
    print(f"Items in all stores AND in recent query: {len(recent_items_in_all_stores):,}")
    
    # Now filter for items that have been present for at least 1 year
    print(f"\n⏰ Finding items present for at least 1 year...")
    
    items_with_duration = []
    
    for item in recent_items_in_all_stores:
        item_data = df[df['item_title'] == item]
        
        # Find the first and last appearance of this item
        first_appearance = item_data['inserted_at'].min()
        last_appearance = item_data['inserted_at'].max()
        
        # Calculate how long this item has been in the dataset
        item_duration = last_appearance - first_appearance
        item_duration_days = item_duration.days
        
        # Check if item has been present for at least 1 year (365 days)
        if item_duration_days >= 365:
            items_with_duration.append({
                'item': item,
                'first_seen': first_appearance,
                'last_seen': last_appearance,
                'duration_days': item_duration_days,
                'duration_years': item_duration_days / 365
            })
    
    # Sort by duration (longest first)
    items_with_duration.sort(key=lambda x: x['duration_days'], reverse=True)
    
    print(f"Items in all stores, recent, AND present for 1+ year: {len(items_with_duration):,}")
    
    if items_with_duration:
        print(f"\nTop 10 longest-running items:")
        print("-" * 60)
        for i, item_info in enumerate(items_with_duration[:10], 1):
            print(f"{i:2d}. {item_info['item'][:50]}...")
            print(f"    Duration: {item_info['duration_years']:.1f} years ({item_info['duration_days']} days)")
            print(f"    First seen: {item_info['first_seen'].strftime('%Y-%m-%d')}")
            print(f"    Last seen: {item_info['last_seen'].strftime('%Y-%m-%d')}")
            print()
    
    return items_with_duration

def step3_analyze_availability_patterns(df, filtered_items):
    """
    STEP 3: Analyze availability patterns for the filtered items
    """
    print("\n📊 STEP 3: Analyzing availability patterns")
    print("=" * 50)
    
    if not filtered_items:
        print("No items to analyze!")
        return
    
    print(f"Analyzing availability for {len(filtered_items)} filtered items...")
    
    # Create monthly periods for analysis
    df['month'] = df['inserted_at'].dt.to_period('M')
    months = sorted(df['month'].unique())
    
    print(f"Data spans {len(months)} months: {months[0]} to {months[-1]}")
    
    # Analyze availability patterns
    availability_summary = []
    
    for item_info in filtered_items[:20]:  # Analyze first 20 items
        item = item_info['item']
        print(f"\n📦 {item[:40]}...")
        
        item_data = df[df['item_title'] == item]
        
        # Calculate overall availability by store
        store_availability = item_data.groupby('store_code')['availability'].mean()
        overall_availability = item_data['availability'].mean()
        
        print(f"  Overall availability: {overall_availability:.1%}")
        print(f"  By store:")
        for store, avail in store_availability.items():
            print(f"    Store {store}: {avail:.1%}")
        
        availability_summary.append({
            'item': item,
            'overall_availability': overall_availability,
            'store_availability': store_availability.to_dict(),
            'duration_years': item_info['duration_years']
        })
    
    return availability_summary

def main():
    """Main function"""
    print("Trader Joe's Product Consistency Analysis")
    print("=" * 60)
    
    # Load data
    df = load_data()
    if df is None:
        return
    
    # Step 1: Find items in all stores
    items_in_all_stores = step1_find_items_in_all_stores(df)
    
    if len(items_in_all_stores) == 0:
        print("No items found in all stores!")
        return
    
    # Step 2: Filter by recency and duration
    filtered_items = step2_filter_by_recency_and_duration(df, items_in_all_stores)
    
    if len(filtered_items) == 0:
        print("No items meet all criteria!")
        return
    
    # Step 3: Analyze availability patterns
    availability_summary = step3_analyze_availability_patterns(df, filtered_items)
    
    # Final summary
    print("\n🎉 FINAL SUMMARY")
    print("=" * 60)
    print(f"Total unique items in dataset: {len(df['item_title'].unique()):,}")
    print(f"Items in all stores: {len(items_in_all_stores):,}")
    print(f"Items meeting all criteria: {len(filtered_items):,}")
    print(f"  - In all stores")
    print(f"  - In most recent data query")
    print(f"  - Present for 1+ year")
    
    if filtered_items:
        print(f"\nTop 5 longest-running core products:")
        for i, item_info in enumerate(filtered_items[:5], 1):
            print(f"{i}. {item_info['item'][:50]}... ({item_info['duration_years']:.1f} years)")

if __name__ == "__main__":
    main() 