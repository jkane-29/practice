#!/usr/bin/env python3
"""
Price Tracker for Tortilla Española
Analyzes price changes by location using the full dataset with caching
"""

import pandas as pd
import dask.dataframe as dd
import os
from datetime import datetime
from csv_cache import CSVCache

def load_full_data_with_cache(csv_path):
    """Load the full dataset using caching for efficient processing"""
    print(f"Loading full dataset with caching: {csv_path}")
    print(f"File size: {os.path.getsize(csv_path) / (1024 * 1024):.2f} MB")
    
    # Initialize cache
    cache = CSVCache()
    
    try:
        # Load with caching
        data = cache.load_csv_with_cache(csv_path, method="dask", low_memory=False)
        if data is not None:
            print(f"Data loaded successfully with caching")
            return data, cache
        else:
            print(f"Error loading data")
            return None, None
    except Exception as e:
        print(f"Error loading with caching: {e}")
        return None, None

def track_tortilla_espanola_prices(cache, csv_path, store_code=701):
    """Track price changes for Tortilla Española at specific store with caching"""
    print(f"\nTracking Tortilla Española prices at store {store_code}")
    print("=" * 70)
    
    try:
        # Try to get filtered data from cache first
        filters = {
            'item_title': 'Tortilla Española',
            'store_code': store_code
        }
        
        print("Filtering data for Tortilla Española with caching...")
        filtered_df = cache.filter_data_with_cache(csv_path, filters)
        
        if filtered_df is None:
            print(f"No data found for Tortilla Española at store {store_code}")
            return None
        
        print(f"Found {len(filtered_df)} records for store {store_code}")
        
        # Sort by date for chronological analysis
        filtered_df['inserted_at'] = pd.to_datetime(filtered_df['inserted_at'])
        filtered_df = filtered_df.sort_values('inserted_at')
        
        # Display results
        print(f"\nPrice History for Tortilla Española at Store {store_code}")
        print("-" * 70)
        
        # Show all records
        for idx, row in filtered_df.iterrows():
            date_str = row['inserted_at'].strftime('%Y-%m-%d %H:%M:%S')
            price = row['retail_price']
            availability = "Available" if row['availability'] == 1.0 else "Unavailable"
            print(f"{date_str} | ${price:.2f} | {availability}")
        
        # Price analysis
        print(f"\nPrice Analysis Summary")
        print("-" * 40)
        print(f"Total price records: {len(filtered_df)}")
        print(f"Current price: ${filtered_df['retail_price'].iloc[-1]:.2f}")
        print(f"Lowest price: ${filtered_df['retail_price'].min():.2f}")
        print(f"Highest price: ${filtered_df['retail_price'].max():.2f}")
        print(f"Average price: ${filtered_df['retail_price'].mean():.2f}")
        
        # Check for price changes
        unique_prices = filtered_df['retail_price'].unique()
        if len(unique_prices) > 1:
            print(f"Price changes detected: {len(unique_prices)} different prices")
            print(f"Price range: ${min(unique_prices):.2f} - ${max(unique_prices):.2f}")
            
            # Show price change timeline
            print(f"\nPrice Change Timeline")
            print("-" * 40)
            current_price = None
            for idx, row in filtered_df.iterrows():
                if current_price != row['retail_price']:
                    date_str = row['inserted_at'].strftime('%Y-%m-%d')
                    print(f"{date_str}: Price changed to ${row['retail_price']:.2f}")
                    current_price = row['retail_price']
        else:
            print(f"Price has remained stable at ${unique_prices[0]:.2f}")
        
        return filtered_df
        
    except Exception as e:
        print(f"Error tracking prices: {e}")
        return None

def compare_all_stores_with_cache(cache, csv_path):
    """Compare prices across all stores for Tortilla Española with caching"""
    print(f"\nComparing Tortilla Española prices across all stores")
    print("=" * 70)
    
    try:
        # Try to get filtered data from cache first
        filters = {'item_title': 'Tortilla Española'}
        
        print("Computing results for all stores with caching...")
        filtered_df = cache.filter_data_with_cache(csv_path, filters)
        
        if filtered_df is None:
            print("No data found for Tortilla Española")
            return None
        
        print(f"Found {len(filtered_df)} total records across all stores")
        
        # Group by store and analyze
        store_analysis = filtered_df.groupby('store_code').agg({
            'retail_price': ['count', 'mean', 'min', 'max'],
            'availability': 'mean'
        }).round(2)
        
        # Flatten column names
        store_analysis.columns = ['record_count', 'avg_price', 'min_price', 'max_price', 'availability_rate']
        
        print(f"\nPrice Comparison by Store")
        print("-" * 60)
        print(f"{'Store':<8} {'Records':<8} {'Avg Price':<10} {'Min':<8} {'Max':<8} {'Avail %':<8}")
        print("-" * 60)
        
        for store_code, row in store_analysis.iterrows():
            avail_pct = row['availability_rate'] * 100
            print(f"{store_code:<8} {row['record_count']:<8} ${row['avg_price']:<9.2f} ${row['min_price']:<7.2f} ${row['max_price']:<7.2f} {avail_pct:<7.1f}%")
        
        return store_analysis
        
    except Exception as e:
        print(f"Error comparing stores: {e}")
        return None

def save_detailed_report(filtered_df, store_code=701):
    """Save a detailed report to CSV"""
    if filtered_df is not None and len(filtered_df) > 0:
        filename = f"tortilla_espanola_store_{store_code}_analysis.csv"
        filtered_df.to_csv(filename, index=False)
        print(f"\nDetailed report saved to: {filename}")
        
        # Also save a summary
        summary_data = {
            'metric': ['Total Records', 'Current Price', 'Lowest Price', 'Highest Price', 'Average Price', 'Price Changes'],
            'value': [
                len(filtered_df),
                f"${filtered_df['retail_price'].iloc[-1]:.2f}",
                f"${filtered_df['retail_price'].min():.2f}",
                f"${filtered_df['retail_price'].max():.2f}",
                f"${filtered_df['retail_price'].mean():.2f}",
                len(filtered_df['retail_price'].unique())
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_filename = f"tortilla_espanola_store_{store_code}_summary.csv"
        summary_df.to_csv(summary_filename, index=False)
        print(f"Summary report saved to: {summary_filename}")

def main():
    """Main function to run the price tracking analysis with caching"""
    print("Tortilla Española Price Tracker with Caching")
    print("=" * 70)
    
    # CSV file path
    csv_path = "/Users/kaner/Downloads/traderjoes-dump-3.csv"
    
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return
    
    # Load the full dataset with caching
    data, cache = load_full_data_with_cache(csv_path)
    if data is None:
        return
    
    # Track prices for store 701 with caching
    store_701_data = track_tortilla_espanola_prices(cache, csv_path, store_code=701)
    
    # Compare across all stores with caching
    all_stores_analysis = compare_all_stores_with_cache(cache, csv_path)
    
    # Save detailed reports
    save_detailed_report(store_701_data, store_code=701)
    
    # Show cache statistics
    print(f"\nCache Statistics:")
    cache.get_cache_stats()
    
    print(f"\nPrice tracking analysis complete!")
    print(f"Check the generated CSV files for detailed data")

if __name__ == "__main__":
    main() 