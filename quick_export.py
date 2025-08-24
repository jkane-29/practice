#!/usr/bin/env python3
"""
Quick Export of Core Products
Fast export of the 100 core products with essential information only
"""

import pandas as pd
import os
from collections import Counter

def quick_export_core_products():
    """
    Quick export of core products - processes data only once
    """
    file_path = "/Users/kaner/Downloads/traderjoes-dump-3.csv"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    print("📁 Quick export of core products...")
    
    # Process data in one pass to collect all needed information
    print("🔍 Processing dataset in one pass...")
    
    stores = set()
    item_store_counts = Counter()
    item_first_seen = {}
    item_last_seen = {}
    item_total_appearances = Counter()
    item_store_first_price = {}
    item_store_last_price = {}
    
    chunk_size = 100000
    total_rows = 0
    
    for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
        total_rows += len(chunk)
        
        # Clean store codes
        chunk['store_code'] = chunk['store_code'].astype(str)
        stores.update(chunk['store_code'].unique())
        
        # Count stores per item
        for item, stores_in_chunk in chunk.groupby('item_title')['store_code'].unique().items():
            item_store_counts[item] = max(item_store_counts[item], len(stores_in_chunk))
        
        # Track first/last appearance, total counts, and store-specific prices
        chunk['inserted_at'] = pd.to_datetime(chunk['inserted_at'], errors='coerce')
        chunk = chunk.dropna(subset=['inserted_at'])
        
        for item, item_chunk in chunk.groupby('item_title'):
            item_total_appearances[item] += len(item_chunk)
            
            # Track first and last appearance
            chunk_min = item_chunk['inserted_at'].min()
            chunk_max = item_chunk['inserted_at'].max()
            
            if item not in item_first_seen or chunk_min < item_first_seen[item]:
                item_first_seen[item] = chunk_min
            
            if item not in item_last_seen or chunk_max > item_last_seen[item]:
                item_last_seen[item] = chunk_max
            
            # Track store-specific first and last prices
            if item not in item_store_first_price:
                item_store_first_price[item] = {}
            if item not in item_store_last_price:
                item_store_last_price[item] = {}
            
            # Process each store separately
            for store in item_chunk['store_code'].unique():
                store_chunk = item_chunk[item_chunk['store_code'] == store]
                
                # First price for this store
                if store not in item_store_first_price[item]:
                    first_store_occurrence = store_chunk[store_chunk['inserted_at'] == store_chunk['inserted_at'].min()].iloc[0]
                    first_price = pd.to_numeric(first_store_occurrence['retail_price'], errors='coerce')
                    item_store_first_price[item][store] = first_price if pd.notna(first_price) else 0
                
                # Last price for this store
                if store not in item_store_last_price[item]:
                    last_store_occurrence = store_chunk[store_chunk['inserted_at'] == store_chunk['inserted_at'].max()].iloc[0]
                    last_price = pd.to_numeric(last_store_occurrence['retail_price'], errors='coerce')
                    item_store_last_price[item][store] = last_price if pd.notna(last_price) else 0
                else:
                    # Update if we found a later occurrence
                    last_store_occurrence = store_chunk[store_chunk['inserted_at'] == store_chunk['inserted_at'].max()].iloc[0]
                    last_price = pd.to_numeric(last_store_occurrence['retail_price'], errors='coerce')
                    if pd.notna(last_price):
                        item_store_last_price[item][store] = last_price
        
        if chunk_num % 10 == 0:
            print(f"  Processed {total_rows:,} rows...")
    
    # Clean store codes
    valid_stores = {code for code in stores if code.isdigit()}
    print(f"✅ Found {len(valid_stores)} valid stores: {sorted(valid_stores)}")
    
    # Find items in all stores
    items_in_all_stores = {item: count for item, count in item_store_counts.items() 
                          if count == len(valid_stores)}
    print(f"✅ Found {len(items_in_all_stores)} items in all stores")
    
    # Find recent items (within last day of the dataset)
    max_time = max(item_last_seen.values())
    recent_cutoff = max_time - pd.Timedelta(days=1)
    
    recent_items = {item for item, last_seen in item_last_seen.items() 
                   if last_seen >= recent_cutoff}
    
    recent_items_in_all_stores = recent_items.intersection(set(items_in_all_stores.keys()))
    print(f"✅ Found {len(recent_items_in_all_stores)} items in all stores AND recent")
    
    # Filter by duration and create export data
    print("⏰ Filtering by duration...")
    
    core_products = []
    
    for item in recent_items_in_all_stores:
        first_seen = item_first_seen[item]
        last_seen = item_last_seen[item]
        duration_days = (last_seen - first_seen).days
        
        if duration_days >= 365:
            # Create base product record
            product_record = {
                'item_title': item,
                'first_appearance': first_seen.strftime('%Y-%m-%d'),
                'last_appearance': last_seen.strftime('%Y-%m-%d'),
                'duration_days': duration_days,
                'duration_years': round(duration_days / 365, 2),
                'total_appearances': item_total_appearances[item],
                'stores_present': len(valid_stores)
            }
            
            # Add store-specific pricing for each store
            for store in sorted(valid_stores):
                first_price = item_store_first_price.get(item, {}).get(store, 0)
                last_price = item_store_last_price.get(item, {}).get(store, 0)
                
                product_record[f'store_{store}_first_price'] = round(first_price, 2)
                product_record[f'store_{store}_last_price'] = round(last_price, 2)
                
                # Calculate price change for this store
                if first_price > 0:
                    price_change = last_price - first_price
                    price_change_percent = (price_change / first_price) * 100
                    product_record[f'store_{store}_price_change'] = round(price_change, 2)
                    product_record[f'store_{store}_price_change_percent'] = round(price_change_percent, 1)
                else:
                    product_record[f'store_{store}_price_change'] = 0
                    product_record[f'store_{store}_price_change_percent'] = 0
            
            core_products.append(product_record)
    
    # Sort by duration
    core_products.sort(key=lambda x: x['duration_days'], reverse=True)
    
    print(f"🎉 Found {len(core_products)} core products meeting all criteria!")
    
    # Export to CSV
    if core_products:
        df = pd.DataFrame(core_products)
        
        output_filename = 'trader_joes_core_products.csv'
        df.to_csv(output_filename, index=False)
        
        print(f"✅ Exported to: {output_filename}")
        print(f"📊 File contains {len(df)} products with {len(df.columns)} columns")
        
        # Show sample
        print(f"\n📋 Sample of exported data:")
        print(df.head(5).to_string())
        
        return df
    
    return None

def main():
    """Main function"""
    print("Trader Joe's Quick Core Products Export")
    print("=" * 50)
    
    # Export the core products
    df = quick_export_core_products()
    
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