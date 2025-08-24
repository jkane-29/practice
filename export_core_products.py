#!/usr/bin/env python3
"""
Export Core Products to CSV
Exports the 100 core products that meet all criteria to a CSV file
"""

import pandas as pd
import os
from collections import Counter

def export_core_products():
    """
    Export the 100 core products to a CSV file
    """
    file_path = "/Users/kaner/Downloads/traderjoes-dump-3.csv"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    print("📁 Loading dataset to export core products...")
    
    # Step 1: Find items in all stores
    print("🔍 Finding items in all stores...")
    stores = set()
    item_store_counts = Counter()
    
    chunk_size = 100000
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk['store_code'] = chunk['store_code'].astype(str)
        stores.update(chunk['store_code'].unique())
        
        for item, stores_in_chunk in chunk.groupby('item_title')['store_code'].unique().items():
            item_store_counts[item] = max(item_store_counts[item], len(stores_in_chunk))
    
    valid_stores = {code for code in stores if code.isdigit()}
    items_in_all_stores = {item: count for item, count in item_store_counts.items() 
                          if count == len(valid_stores)}
    
    print(f"✅ Found {len(items_in_all_stores)} items in all stores")
    
    # Step 2: Find recent items
    print("📅 Finding recent items...")
    min_time = None
    max_time = None
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk = chunk[chunk['inserted_at'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}')]
        if len(chunk) > 0:
            chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
            chunk = chunk.dropna(subset=['inserted_at'])
            
            if len(chunk) > 0:
                chunk_min = chunk['inserted_at'].min()
                chunk_max = chunk['inserted_at'].max()
                
                if min_time is None or chunk_min < min_time:
                    min_time = chunk_min
                if max_time is None or chunk_max > max_time:
                    max_time = chunk_max
    
    recent_cutoff = max_time - pd.Timedelta(days=1)
    recent_items = set()
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk = chunk[chunk['inserted_at'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}')]
        if len(chunk) > 0:
            chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
            chunk = chunk.dropna(subset=['inserted_at'])
            
            if len(chunk) > 0:
                recent_chunk = chunk[chunk['inserted_at'] >= recent_cutoff]
                recent_items.update(recent_chunk['item_title'].unique())
    
    recent_items_in_all_stores = recent_items.intersection(set(items_in_all_stores.keys()))
    print(f"✅ Found {len(recent_items_in_all_stores)} items in all stores AND recent")
    
    # Step 3: Check duration and collect detailed data
    print("⏰ Checking duration and collecting data...")
    
    core_products_data = []
    
    for item in list(recent_items_in_all_stores):
        print(f"  Processing: {item[:50]}...")
        
        first_seen = None
        last_seen = None
        total_appearances = 0
        store_data = {}
        
        # Collect all data for this item
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk = chunk[chunk['inserted_at'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}')]
            if len(chunk) > 0:
                chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
                chunk = chunk.dropna(subset=['inserted_at'])
                
                if len(chunk) > 0:
                    item_chunk = chunk[chunk['item_title'] == item]
                    
                    if len(item_chunk) > 0:
                        total_appearances += len(item_chunk)
                        
                        # Track first and last appearance
                        chunk_min = item_chunk['inserted_at'].min()
                        chunk_max = item_chunk['inserted_at'].max()
                        
                        if first_seen is None or chunk_min < first_seen:
                            first_seen = chunk_min
                        if last_seen is None or chunk_max > last_seen:
                            last_seen = chunk_max
                        
                        # Collect store-specific data
                        for store in valid_stores:
                            store_chunk = item_chunk[item_chunk['store_code'] == store]
                            if len(store_chunk) > 0:
                                if store not in store_data:
                                    store_data[store] = {
                                        'appearances': 0,
                                        'total_availability': 0,
                                        'prices': []
                                    }
                                
                                store_data[store]['appearances'] += len(store_chunk)
                                # Convert availability to numeric and handle any non-numeric values
                                availability_sum = pd.to_numeric(store_chunk['availability'], errors='coerce').sum()
                                if pd.notna(availability_sum):
                                    store_data[store]['total_availability'] += availability_sum
                                # Convert prices to numeric and handle any non-numeric values
                                prices = pd.to_numeric(store_chunk['retail_price'], errors='coerce').dropna().tolist()
                                store_data[store]['prices'].extend(prices)
        
        if first_seen and last_seen:
            duration_days = (last_seen - first_seen).days
            
            if duration_days >= 365:
                # Calculate store-specific statistics
                store_stats = {}
                for store in valid_stores:
                    if store in store_data:
                        store_info = store_data[store]
                        avg_price = sum(store_info['prices']) / len(store_info['prices']) if store_info['prices'] else 0
                        availability_rate = store_info['total_availability'] / store_info['appearances'] if store_info['appearances'] > 0 else 0
                        
                        store_stats[f'store_{store}_appearances'] = store_info['appearances']
                        store_stats[f'store_{store}_availability_rate'] = availability_rate
                        store_stats[f'store_{store}_avg_price'] = avg_price
                    else:
                        store_stats[f'store_{store}_appearances'] = 0
                        store_stats[f'store_{store}_availability_rate'] = 0
                        store_stats[f'store_{store}_avg_price'] = 0
                
                # Create product record
                product_record = {
                    'item_title': item,
                    'first_appearance': first_seen.strftime('%Y-%m-%d'),
                    'last_appearance': last_seen.strftime('%Y-%m-%d'),
                    'duration_days': duration_days,
                    'duration_years': round(duration_days / 365, 2),
                    'total_appearances': total_appearances,
                    'stores_present': len(valid_stores),
                    **store_stats
                }
                
                core_products_data.append(product_record)
                print(f"    ✅ Added to export ({duration_days} days)")
    
    # Sort by duration (longest first)
    core_products_data.sort(key=lambda x: x['duration_days'], reverse=True)
    
    print(f"\n🎉 Ready to export {len(core_products_data)} core products!")
    
    # Step 4: Export to CSV
    if core_products_data:
        df = pd.DataFrame(core_products_data)
        
        # Reorder columns for better readability
        column_order = [
            'item_title', 'duration_years', 'duration_days', 
            'first_appearance', 'last_appearance', 'total_appearances', 'stores_present'
        ]
        
        # Add store columns
        for store in sorted(valid_stores):
            column_order.extend([
                f'store_{store}_appearances',
                f'store_{store}_availability_rate', 
                f'store_{store}_avg_price'
            ])
        
        df = df[column_order]
        
        # Export to CSV
        output_filename = 'trader_joes_core_products.csv'
        df.to_csv(output_filename, index=False)
        
        print(f"✅ Exported to: {output_filename}")
        print(f"📊 File contains {len(df)} products with {len(df.columns)} columns")
        
        # Show sample of exported data
        print(f"\n📋 Sample of exported data:")
        print(df.head(3).to_string())
        
        return df
    
    return None

def main():
    """Main function"""
    print("Trader Joe's Core Products Export")
    print("=" * 50)
    
    # Export the core products
    df = export_core_products()
    
    if df is not None:
        print(f"\n🎉 Export complete!")
        print(f"📁 File saved as: trader_joes_core_products.csv")
        print(f"📊 Contains {len(df)} core products")
        print(f"🏪 Covers {df['stores_present'].iloc[0]} stores")
        print(f"⏰ Time span: {df['duration_years'].min():.1f} to {df['duration_years'].max():.1f} years")
    else:
        print("\n❌ Export failed!")

if __name__ == "__main__":
    main() 