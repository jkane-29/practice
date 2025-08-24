#!/usr/bin/env python3
"""
Comprehensive Product Analysis for Trader Joe's Data
This script analyzes product consistency across stores and time periods
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

def load_large_dataset():
    """
    Load the large dataset - this function can be modified to load your full dataset
    """
    print("🔍 LOADING DATASET")
    print("=" * 60)
    
    # Try to find the largest available dataset
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    
    if not csv_files:
        print("❌ No CSV files found!")
        return None
    
    # Find the largest CSV file (likely the main dataset)
    largest_file = max(csv_files, key=lambda x: os.path.getsize(x))
    file_size_mb = os.path.getsize(largest_file) / (1024 * 1024)
    
    print(f"Found dataset: {largest_file}")
    print(f"File size: {file_size_mb:.2f} MB")
    
    try:
        # Load the dataset
        print("Loading data...")
        df = pd.read_csv(largest_file)
        print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"❌ Error loading {largest_file}: {e}")
        return None

def analyze_data_structure(df):
    """
    Analyze the structure of the dataset
    """
    print("\n📊 DATASET STRUCTURE ANALYSIS")
    print("=" * 60)
    
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
    
    # Check for timestamp/date columns
    date_columns = []
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower() or 'inserted' in col.lower():
            date_columns.append(col)
    
    if date_columns:
        print(f"\n📅 Date/time columns found: {date_columns}")
        for col in date_columns:
            print(f"   {col}: {df[col].dtype}")
            if df[col].dtype == 'object':
                # Try to parse dates
                try:
                    df[col] = pd.to_datetime(df[col])
                    print(f"   ✅ Converted {col} to datetime")
                except:
                    print(f"   ❌ Could not convert {col} to datetime")
    
    # Check store information
    if 'store_code' in df.columns:
        stores = df['store_code'].unique()
        print(f"\n🏪 Stores found: {sorted(stores)}")
        print(f"   Total stores: {len(stores)}")
    
    return df

def find_items_in_all_stores(df):
    """
    Find items that are listed in ALL stores (regardless of availability)
    """
    print("\n🎯 FINDING ITEMS IN ALL STORES")
    print("=" * 60)
    
    if 'store_code' not in df.columns or 'item_title' not in df.columns:
        print("❌ Missing required columns: store_code or item_title")
        return None
    
    # Get unique stores
    all_stores = sorted(df['store_code'].unique())
    print(f"Looking for items that appear in all {len(all_stores)} stores: {all_stores}")
    
    # Count how many stores each item appears in
    store_counts = df.groupby('item_title')['store_code'].nunique()
    
    # Find items that appear in ALL stores
    items_in_all_stores = store_counts[store_counts == len(all_stores)]
    
    print(f"\n✅ Found {len(items_in_all_stores)} items that appear in ALL stores")
    
    if len(items_in_all_stores) > 0:
        print("\nTop 20 items in all stores:")
        print("-" * 40)
        for i, (item, count) in enumerate(items_in_all_stores.head(20).items(), 1):
            print(f"{i:2d}. {item}")
    
    return items_in_all_stores

def analyze_availability_over_time(df, items_in_all_stores):
    """
    Analyze availability of items over time periods
    """
    print("\n📈 ANALYZING AVAILABILITY OVER TIME")
    print("=" * 60)
    
    if 'inserted_at' not in df.columns:
        print("❌ No timestamp column found for time analysis")
        return None
    
    # Ensure timestamp is datetime
    if df['inserted_at'].dtype != 'datetime64[ns]':
        try:
            df['inserted_at'] = pd.to_datetime(df['inserted_at'])
        except:
            print("❌ Could not convert inserted_at to datetime")
            return None
    
    # Find the time range
    earliest_time = df['inserted_at'].min()
    latest_time = df['inserted_at'].max()
    
    print(f"Data spans from: {earliest_time} to {latest_time}")
    
    # Create time periods (e.g., by month)
    df['month'] = df['inserted_at'].dt.to_period('M')
    months = sorted(df['month'].unique())
    
    print(f"Time periods: {len(months)} months")
    print(f"First month: {months[0]}, Last month: {months[-1]}")
    
    # Analyze availability for items in all stores
    availability_analysis = {}
    
    for item in items_in_all_stores.index[:10]:  # Analyze first 10 items
        print(f"\n📦 Analyzing: {item[:50]}...")
        
        item_data = df[df['item_title'] == item]
        availability_analysis[item] = {}
        
        for month in months:
            month_data = item_data[item_data['month'] == month]
            if len(month_data) > 0:
                # Calculate availability by store for this month
                store_availability = month_data.groupby('store_code')['availability'].mean()
                availability_analysis[item][month] = store_availability.to_dict()
    
    return availability_analysis, months

def find_consistently_available_items(df, items_in_all_stores, months):
    """
    Find items that have been consistently available since the first refresh
    """
    print("\n🔄 FINDING CONSISTENTLY AVAILABLE ITEMS")
    print("=" * 60)
    
    if 'availability' not in df.columns:
        print("❌ No availability column found")
        return None
    
    consistently_available = []
    
    for item in items_in_all_stores.index:
        print(f"Checking consistency for: {item[:50]}...")
        
        item_data = df[df['item_title'] == item]
        
        # Check if item has been available in all stores since first refresh
        is_consistent = True
        
        for month in months:
            month_data = item_data[item_data['month'] == month]
            if len(month_data) > 0:
                # Check if available in all stores this month
                store_availability = month_data.groupby('store_code')['availability'].sum()
                if len(store_availability) < len(df['store_code'].unique()):
                    is_consistent = False
                    break
                # Check if all stores have availability > 0
                if (store_availability == 0).any():
                    is_consistent = False
                    break
        
        if is_consistent:
            consistently_available.append(item)
            print(f"   ✅ {item[:50]}... - CONSISTENTLY AVAILABLE")
        else:
            print(f"   ❌ {item[:50]}... - NOT consistently available")
    
    print(f"\n🎉 Found {len(consistently_available)} consistently available items!")
    
    return consistently_available

def create_consistency_visualizations(df, items_in_all_stores, consistently_available, months):
    """
    Create visualizations showing product consistency
    """
    print("\n📊 CREATING CONSISTENCY VISUALIZATIONS")
    print("=" * 60)
    
    try:
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create a figure with multiple subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Trader Joe\'s Product Consistency Analysis', fontsize=16)
        
        # Plot 1: Items per store count distribution
        store_counts = df.groupby('item_title')['store_code'].nunique()
        axes[0, 0].hist(store_counts.values, bins=range(1, len(df['store_code'].unique())+2), 
                        alpha=0.7, edgecolor='black', color='lightblue')
        axes[0, 0].set_title('Distribution of Items by Store Count')
        axes[0, 0].set_xlabel('Number of Stores')
        axes[0, 0].set_ylabel('Number of Items')
        
        # Plot 2: Availability over time for consistently available items
        if consistently_available and len(consistently_available) > 0:
            sample_item = consistently_available[0]
            item_data = df[df['item_title'] == sample_item]
            
            # Group by month and store
            monthly_availability = item_data.groupby(['month', 'store_code'])['availability'].mean().unstack()
            
            monthly_availability.plot(ax=axes[0, 1], marker='o')
            axes[0, 1].set_title(f'Availability Over Time: {sample_item[:30]}...')
            axes[0, 1].set_xlabel('Month')
            axes[0, 1].set_ylabel('Availability Rate')
            axes[0, 1].legend(title='Store Code')
        
        # Plot 3: Store coverage comparison
        store_coverage = df.groupby('store_code')['item_title'].nunique()
        store_coverage.plot(kind='bar', ax=axes[1, 0], color='orange')
        axes[1, 0].set_title('Total Items per Store')
        axes[1, 0].set_xlabel('Store Code')
        axes[1, 0].set_ylabel('Number of Items')
        
        # Plot 4: Consistency summary
        total_items = len(df['item_title'].unique())
        items_in_all_stores_count = len(items_in_all_stores)
        consistently_available_count = len(consistently_available) if consistently_available else 0
        
        categories = ['Total Items', 'In All Stores', 'Consistently Available']
        counts = [total_items, items_in_all_stores_count, consistently_available_count]
        
        axes[1, 1].bar(categories, counts, color=['lightgray', 'lightblue', 'lightgreen'])
        axes[1, 1].set_title('Product Consistency Summary')
        axes[1, 1].set_ylabel('Number of Items')
        
        # Add value labels on bars
        for i, count in enumerate(counts):
            axes[1, 1].text(i, count + max(counts)*0.01, str(count), ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('product_consistency_analysis.png', dpi=300, bbox_inches='tight')
        print("✅ Consistency visualization saved as 'product_consistency_analysis.png'")
        
    except Exception as e:
        print(f"❌ Error creating visualization: {e}")

def main():
    """Main function to run the comprehensive analysis"""
    print("Trader Joe's Comprehensive Product Consistency Analysis")
    print("=" * 80)
    
    # Load the dataset
    df = load_large_dataset()
    if df is None:
        print("❌ Cannot proceed without data.")
        return
    
    # Analyze data structure
    df = analyze_data_structure(df)
    
    # Find items in all stores
    items_in_all_stores = find_items_in_all_stores(df)
    if items_in_all_stores is None or len(items_in_all_stores) == 0:
        print("❌ No items found in all stores.")
        return
    
    # Analyze availability over time
    availability_analysis, months = analyze_availability_over_time(df, items_in_all_stores)
    
    # Find consistently available items
    consistently_available = find_consistently_available_items(df, items_in_all_stores, months)
    
    # Create visualizations
    create_consistency_visualizations(df, items_in_all_stores, consistently_available, months)
    
    # Summary
    print("\n🎉 ANALYSIS COMPLETE!")
    print("=" * 60)
    print(f"Total items analyzed: {len(df['item_title'].unique()):,}")
    print(f"Items in all stores: {len(items_in_all_stores):,}")
    print(f"Consistently available: {len(consistently_available) if consistently_available else 0:,}")
    
    if consistently_available:
        print(f"\nTop 10 consistently available items:")
        for i, item in enumerate(consistently_available[:10], 1):
            print(f"{i:2d}. {item}")

if __name__ == "__main__":
    main() 