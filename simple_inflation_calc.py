#!/usr/bin/env python3
"""
Simple Command-Line Inflation Calculator
Quick inflation calculations for Trader Joe's baskets
"""

import pandas as pd
import os

def load_data():
    """Load the core products data"""
    csv_file = 'trader_joes_core_products.csv'
    if not os.path.exists(csv_file):
        print(f"❌ Core products file not found: {csv_file}")
        return None
    
    df = pd.read_csv(csv_file)
    print(f"✅ Loaded {len(df)} products")
    
    # Get store codes
    store_columns = [col for col in df.columns if col.startswith('store_') and '_first_price' in col]
    store_codes = [col.split('_')[1] for col in store_columns]
    print(f"✅ Available stores: {store_codes}")
    
    return df, store_codes

def search_products(df, search_term, store_code):
    """Search for products by name and store"""
    if search_term:
        filtered_df = df[df['item_title'].str.lower().str.contains(search_term.lower(), na=False)]
    else:
        filtered_df = df
    
    # Get products available at selected store
    first_price_col = f'store_{store_code}_first_price'
    last_price_col = f'store_{store_code}_last_price'
    
    available_products = filtered_df[
        (filtered_df[first_price_col] > 0) & 
        (filtered_df[last_price_col] > 0)
    ]
    
    return available_products

def display_products(products, store_code):
    """Display available products"""
    if len(products) == 0:
        print("No products found matching your search.")
        return
    
    print(f"\n📋 Available products at Store {store_code}:")
    print("-" * 80)
    
    for i, (_, row) in enumerate(products.iterrows(), 1):
        first_price = row[f'store_{store_code}_first_price']
        last_price = row[f'store_{store_code}_last_price']
        price_change = last_price - first_price
        price_change_pct = (price_change / first_price * 100) if first_price > 0 else 0
        
        print(f"{i:3d}. {row['item_title'][:60]:<60} | ${first_price:5.2f} → ${last_price:5.2f} ({price_change_pct:+6.1f}%)")

def select_products(products, store_code):
    """Let user select products for basket"""
    basket = []
    
    print(f"\n🛒 Select products for your basket (Store {store_code}):")
    print("Enter product numbers separated by commas, or 'done' to finish:")
    
    while True:
        choice = input("Selection: ").strip()
        
        if choice.lower() == 'done':
            break
        
        try:
            indices = [int(x.strip()) - 1 for x in choice.split(',')]
            for idx in indices:
                if 0 <= idx < len(products):
                    row = products.iloc[idx]
                    
                    basket_item = {
                        'title': row['item_title'],
                        'first_price': row[f'store_{store_code}_first_price'],
                        'last_price': row[f'store_{store_code}_last_price'],
                        'price_change': row[f'store_{store_code}_price_change'],
                        'price_change_percent': row[f'store_{store_code}_price_change_percent'],
                        'first_date': row['first_appearance'],
                        'last_date': row['last_appearance'],
                        'duration_days': row['duration_days']
                    }
                    
                    basket.append(basket_item)
                    print(f"✅ Added: {basket_item['title'][:50]}...")
                else:
                    print(f"❌ Invalid selection: {idx + 1}")
        except ValueError:
            print("❌ Please enter valid numbers separated by commas")
    
    return basket

def calculate_inflation(basket, store_code):
    """Calculate weighted inflation for the basket"""
    if not basket:
        print("❌ Basket is empty!")
        return
    
    print(f"\n📊 INFLATION ANALYSIS FOR STORE {store_code}")
    print("=" * 60)
    
    # Calculate totals
    total_first_value = sum(item['first_price'] for item in basket)
    total_last_value = sum(item['last_price'] for item in basket)
    
    # Calculate weighted average price change
    weighted_price_changes = []
    total_weight = 0
    
    for item in basket:
        weight = item['first_price']  # Weight by initial price
        total_weight += weight
        weighted_price_changes.append(item['price_change_percent'] * weight)
    
    weighted_avg_change = sum(weighted_price_changes) / total_weight if total_weight > 0 else 0
    
    # Calculate time-weighted inflation (annualized)
    avg_duration_days = sum(item['duration_days'] for item in basket) / len(basket)
    annualized_inflation = weighted_avg_change * (365 / avg_duration_days) if avg_duration_days > 0 else 0
    
    # Display results
    print(f"Basket Composition:")
    print(f"  Number of items: {len(basket)}")
    print(f"  Total initial value: ${total_first_value:.2f}")
    print(f"  Total final value: ${total_last_value:.2f}")
    print()
    
    print(f"Price Changes:")
    print(f"  Total basket change: ${total_last_value - total_first_value:+.2f}")
    print(f"  Percentage change: {((total_last_value - total_first_value) / total_first_value * 100):+.2f}%")
    print()
    
    print(f"Weighted Analysis:")
    print(f"  Average duration: {avg_duration_days:.1f} days")
    print(f"  Weighted average price change: {weighted_avg_change:+.2f}%")
    print(f"  Annualized inflation rate: {annualized_inflation:+.2f}%")
    print()
    
    # Categorize inflation
    if annualized_inflation > 2:
        category = "HIGH INFLATION"
    elif annualized_inflation > 0:
        category = "MODERATE INFLATION"
    elif annualized_inflation > -2:
        category = "PRICE STABILITY"
    else:
        category = "DEFLATION"
    
    print(f"Category: {category}")
    
    # Individual item analysis
    print(f"\nIndividual Item Analysis:")
    print("-" * 40)
    
    for i, item in enumerate(basket, 1):
        weight_pct = (item['first_price'] / total_first_value * 100) if total_first_value > 0 else 0
        print(f"{i}. {item['title'][:40]}...")
        print(f"   Weight: {weight_pct:.1f}% | Change: {item['price_change_percent']:+.1f}%")

def main():
    """Main function"""
    print("Trader Joe's Inflation Calculator")
    print("=" * 50)
    
    # Load data
    data = load_data()
    if data is None:
        return
    
    df, store_codes = data
    
    # Select store
    print(f"\n🏪 Select a store:")
    for i, store in enumerate(store_codes, 1):
        print(f"{i}. Store {store}")
    
    while True:
        try:
            store_choice = int(input(f"Enter store number (1-{len(store_codes)}): ")) - 1
            if 0 <= store_choice < len(store_codes):
                store_code = store_codes[store_choice]
                break
            else:
                print("❌ Invalid store number")
        except ValueError:
            print("❌ Please enter a valid number")
    
    print(f"✅ Selected Store {store_code}")
    
    # Search and select products
    while True:
        search_term = input(f"\n🔍 Search products (or press Enter to see all): ").strip()
        
        products = search_products(df, search_term, store_code)
        display_products(products, store_code)
        
        if len(products) > 0:
            basket = select_products(products, store_code)
            
            if basket:
                calculate_inflation(basket, store_code)
                
                # Ask if user wants to create another basket
                another = input(f"\n🔄 Create another basket? (y/n): ").strip().lower()
                if another != 'y':
                    break
            else:
                print("❌ No products selected")
                break
        else:
            print("❌ No products found. Try a different search term.")

if __name__ == "__main__":
    main() 