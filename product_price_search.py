#!/usr/bin/env python3
"""
Product Price Search - Learn to Search for Price Changes
This script shows you how to find price information for specific products
"""

import pandas as pd

def load_data():
    """Load the Trader Joe's data"""
    try:
        import glob
        sample_files = glob.glob("trader_joes_sample_*.csv")
        if sample_files:
            sample_file = max(sample_files, key=lambda x: os.path.getctime(x))
            print(f"Loading data from: {sample_file}")
            return pd.read_csv(sample_file)
        else:
            print("No sample file found!")
            return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def search_product_by_name(df, product_name):
    """
    Search for a product by name (partial match)
    
    Parameters:
    df: pandas DataFrame with the data
    product_name: string to search for (can be partial)
    
    Returns:
    DataFrame with matching products
    """
    print(f"🔍 Searching for products containing: '{product_name}'")
    print("=" * 60)
    
    # Method 1: Simple string contains (case-insensitive)
    # This finds any product where the name contains your search term
    matches = df[df['item_title'].str.contains(product_name, case=False, na=False)]
    
    print(f"Found {len(matches)} matching products:")
    print("-" * 40)
    
    if len(matches) > 0:
        # Show the results in a nice format
        for i, (index, row) in enumerate(matches.iterrows(), 1):
            print(f"{i}. {row['item_title']}")
            print(f"   Price: ${row['retail_price']:.2f}")
            print(f"   Store: {row['store_code']}")
            print(f"   Available: {'Yes' if row['availability'] == 1 else 'No'}")
            print()
    
    return matches

def find_price_changes_for_product(df, product_name):
    """
    Find all price variations for a specific product across stores
    
    Parameters:
    df: pandas DataFrame with the data
    product_name: exact product name to search for
    
    Returns:
    DataFrame with price variations
    """
    print(f"💰 Finding price changes for: '{product_name}'")
    print("=" * 60)
    
    # Find exact matches for the product
    exact_matches = df[df['item_title'] == product_name]
    
    if len(exact_matches) == 0:
        print("❌ No exact matches found. Try searching first to see available products.")
        return None
    
    print(f"Found {len(exact_matches)} instances of this product:")
    print("-" * 40)
    
    # Group by store to see price variations
    store_prices = exact_matches.groupby('store_code').agg({
        'retail_price': ['count', 'mean', 'min', 'max'],
        'availability': 'mean'
    }).round(2)
    
    print("Price summary by store:")
    print(store_prices)
    
    # Show individual instances
    print(f"\nDetailed breakdown:")
    print("-" * 40)
    for i, (index, row) in enumerate(exact_matches.iterrows(), 1):
        print(f"{i}. Store {row['store_code']}: ${row['retail_price']:.2f} "
              f"({'Available' if row['availability'] == 1 else 'Not Available'})")
    
    # Calculate price statistics
    prices = exact_matches['retail_price']
    if len(prices) > 1:
        print(f"\n📊 Price Statistics:")
        print(f"   Cheapest: ${prices.min():.2f}")
        print(f"   Most expensive: ${prices.max():.2f}")
        print(f"   Price range: ${prices.max() - prices.min():.2f}")
        print(f"   Average price: ${prices.mean():.2f}")
    
    return exact_matches

def interactive_search(df):
    """
    Interactive search function - you can type product names to search
    """
    print("🎯 INTERACTIVE PRODUCT SEARCH")
    print("=" * 60)
    print("Type product names to search (or 'quit' to exit)")
    print("Examples: 'chocolate', 'bread', 'cheese', 'coffee'")
    print()
    
    while True:
        # Get user input
        search_term = input("Enter product name to search: ").strip()
        
        if search_term.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        if not search_term:
            print("Please enter a search term.")
            continue
        
        # Search for the product
        matches = search_product_by_name(df, search_term)
        
        if len(matches) > 0:
            # Ask if they want to see price changes for a specific product
            print(f"\nWould you like to see price changes for a specific product?")
            print("Enter the exact product name from the list above, or 'search' to search again.")
            
            choice = input("Your choice: ").strip()
            
            if choice.lower() != 'search':
                # Find price changes for the selected product
                find_price_changes_for_product(df, choice)
        
        print("\n" + "="*60 + "\n")

def main():
    """Main function"""
    print("Trader Joe's Product Price Search")
    print("=" * 60)
    
    # Load the data
    df = load_data()
    
    if df is None:
        print("Cannot proceed without data.")
        return
    
    # Show some example searches
    print("Let's start with some example searches:")
    print()
    
    # Example 1: Search for chocolate products
    print("Example 1: Searching for 'chocolate' products")
    chocolate_matches = search_product_by_name(df, 'chocolate')
    
    if len(chocolate_matches) > 0:
        print("\nNow let's look at price changes for a specific chocolate product:")
        # Get the first chocolate product found
        first_chocolate = chocolate_matches.iloc[0]['item_title']
        find_price_changes_for_product(df, first_chocolate)
    
    print("\n" + "="*60 + "\n")
    
    # Start interactive search
    interactive_search(df)

if __name__ == "__main__":
    import os
    main() 