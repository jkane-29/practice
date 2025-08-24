#!/usr/bin/env python3
"""
My Search Functions - Template for you to modify and experiment
Copy and modify these functions to create your own searches!
"""

import pandas as pd

def load_data():
    """Load the Trader Joe's data"""
    import glob
    sample_files = glob.glob("trader_joes_sample_*.csv")
    if sample_files:
        sample_file = max(sample_files, key=lambda x: os.path.getctime(x))
        return pd.read_csv(sample_file)
    return None

# ============================================================================
# YOUR TURN! Modify these functions below:
# ============================================================================

def my_product_search(df, search_term):
    """
    YOUR FUNCTION: Search for products
    
    TODO: Modify this function to search however you want!
    """
    print(f"🔍 My search for: {search_term}")
    
    # Method 1: Find products containing the search term
    # YOUR CODE HERE - try different search methods:
    
    # Method 2: Find products by price range
    # YOUR CODE HERE - try filtering by price:
    
    # Method 3: Find products by store
    # YOUR CODE HERE - try filtering by store:
    
    # Method 4: Find available products only
    # YOUR CODE HERE - try filtering by availability:
    
    return df  # Return your filtered results

def my_price_analysis(df, product_name):
    """
    YOUR FUNCTION: Analyze prices for a specific product
    
    TODO: Modify this to show price information however you want!
    """
    print(f"💰 My price analysis for: {product_name}")
    
    # YOUR CODE HERE - find the product and analyze its prices:
    
    # YOUR CODE HERE - calculate price statistics:
    
    # YOUR CODE HERE - show price differences between stores:
    
    return df  # Return your analysis results

def my_custom_search(df):
    """
    YOUR FUNCTION: Create your own custom search!
    
    TODO: This is where you can be creative - what do you want to find?
    """
    print("🎯 My custom search!")
    
    # Example ideas:
    # - Find all products under $5 that are available
    # - Find products with "organic" in the name
    # - Find the cheapest product in each store
    # - Find products that appear in multiple stores
    
    # YOUR CODE HERE - be creative!
    
    return df

# ============================================================================
# TEST YOUR FUNCTIONS HERE:
# ============================================================================

def test_my_functions():
    """Test your modified functions"""
    print("Testing your search functions...")
    
    # Load data
    df = load_data()
    if df is None:
        print("No data found!")
        return
    
    print(f"Loaded {len(df)} products")
    
    # Test your functions
    print("\n1. Testing my_product_search:")
    my_product_search(df, "chocolate")
    
    print("\n2. Testing my_price_analysis:")
    my_price_analysis(df, "Milk Chocolate Peanut Butter Cups")
    
    print("\n3. Testing my_custom_search:")
    my_custom_search(df)

if __name__ == "__main__":
    import os
    test_my_functions() 