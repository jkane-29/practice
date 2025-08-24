#!/usr/bin/env python3
"""
Price Variance Analysis for Trader Joe's Products
Finds products with the highest price differences between stores
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def load_data():
    """Load the Trader Joe's sample data"""
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

def analyze_price_variance(df):
    """Analyze price variance across stores for each product"""
    print("🔍 ANALYZING PRICE VARIANCE ACROSS STORES")
    print("=" * 60)
    
    # Group by product name and store to get price data
    product_store_prices = df.groupby(['item_title', 'store_code'])['retail_price'].first().reset_index()
    
    # Pivot to get stores as columns
    price_pivot = product_store_prices.pivot(index='item_title', columns='store_code', values='retail_price')
    
    print(f"Found {len(price_pivot)} unique products across {len(price_pivot.columns)} stores")
    print(f"Store codes: {list(price_pivot.columns)}")
    
    # Calculate statistics for each product
    price_stats = price_pivot.agg(['count', 'mean', 'std', 'min', 'max'], axis=1)
    
    # Calculate price range (max - min) for each product
    price_stats['price_range'] = price_stats['max'] - price_stats['min']
    
    # Calculate coefficient of variation (std/mean) for relative variance
    price_stats['cv'] = (price_stats['std'] / price_stats['mean']).fillna(0)
    
    # Filter products that appear in multiple stores
    multi_store_products = price_stats[price_stats['count'] > 1].copy()
    
    print(f"\nProducts appearing in multiple stores: {len(multi_store_products)}")
    
    return price_pivot, price_stats, multi_store_products

def find_highest_variance_products(price_stats, price_pivot):
    """Find products with highest price variance"""
    print("\n🏆 TOP 20 PRODUCTS BY PRICE VARIANCE")
    print("=" * 60)
    
    # Sort by price range (absolute difference)
    top_by_range = price_stats.sort_values('price_range', ascending=False).head(20)
    
    print("Ranked by absolute price difference (max - min):")
    print("-" * 60)
    
    for i, (product, stats) in enumerate(top_by_range.iterrows(), 1):
        price_range = stats['price_range']
        mean_price = stats['mean']
        cv = stats['cv']
        count = stats['count']
        
        print(f"{i:2d}. {product[:60]:<60}")
        print(f"    Price range: ${price_range:.2f} | Mean: ${mean_price:.2f} | CV: {cv:.2%} | Stores: {count}")
        
        # Show actual prices by store
        store_prices = price_pivot.loc[product].dropna()
        price_details = [f"Store {store}: ${price:.2f}" for store, price in store_prices.items()]
        print(f"    Prices: {', '.join(price_details)}")
        print()
    
    return top_by_range

def find_highest_relative_variance(price_stats, price_pivot):
    """Find products with highest relative variance (coefficient of variation)"""
    print("\n📊 TOP 20 PRODUCTS BY RELATIVE PRICE VARIANCE")
    print("=" * 60)
    
    # Sort by coefficient of variation (relative variance)
    top_by_cv = price_stats.sort_values('cv', ascending=False).head(20)
    
    print("Ranked by coefficient of variation (std/mean):")
    print("-" * 60)
    
    for i, (product, stats) in enumerate(top_by_cv.iterrows(), 1):
        cv = stats['cv']
        price_range = stats['price_range']
        mean_price = stats['mean']
        count = stats['count']
        
        print(f"{i:2d}. {product[:60]:<60}")
        print(f"    CV: {cv:.2%} | Price range: ${price_range:.2f} | Mean: ${mean_price:.2f} | Stores: {count}")
        
        # Show actual prices by store
        store_prices = price_pivot.loc[product].dropna()
        price_details = [f"Store {store}: ${price:.2f}" for store, price in store_prices.items()]
        print(f"    Prices: {', '.join(price_details)}")
        print()
    
    return top_by_cv

def analyze_store_price_patterns(price_pivot, price_stats):
    """Analyze which stores tend to have higher/lower prices"""
    print("\n🏪 STORE PRICE PATTERNS")
    print("=" * 60)
    
    # Calculate store-level statistics
    store_stats = {}
    
    for store in price_pivot.columns:
        store_prices = price_pivot[store].dropna()
        store_stats[store] = {
            'count': len(store_prices),
            'mean': store_prices.mean(),
            'median': store_prices.median(),
            'std': store_prices.std()
        }
    
    print("Store price statistics:")
    print("-" * 40)
    for store, stats in store_stats.items():
        print(f"Store {store}: {stats['count']} products, "
              f"avg: ${stats['mean']:.2f}, median: ${stats['median']:.2f}")
    
    # Find which store is most expensive/cheapest for each product
    print(f"\nStore price positioning analysis:")
    print("-" * 40)
    
    for store in price_pivot.columns:
        # Count how many times this store has the highest price
        highest_count = 0
        lowest_count = 0
        
        for product in price_pivot.index:
            prices = price_pivot.loc[product].dropna()
            if len(prices) > 1:  # Only for products in multiple stores
                if store in prices.index:  # Check if store exists for this product
                    if prices[store] == prices.max():
                        highest_count += 1
                    if prices[store] == prices.min():
                        lowest_count += 1
        
        print(f"Store {store}: Highest price {highest_count} times, Lowest price {lowest_count} times")
    
    return store_stats

def create_variance_visualizations(price_stats, price_pivot):
    """Create visualizations for price variance analysis"""
    print("\n📈 CREATING PRICE VARIANCE VISUALIZATIONS")
    print("=" * 60)
    
    try:
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create a figure with multiple subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Trader Joe\'s Price Variance Analysis', fontsize=16)
        
        # Plot 1: Distribution of price ranges
        price_ranges = price_stats[price_stats['count'] > 1]['price_range']
        axes[0, 0].hist(price_ranges, bins=30, alpha=0.7, edgecolor='black', color='lightblue')
        axes[0, 0].set_title('Distribution of Price Ranges Across Stores')
        axes[0, 0].set_xlabel('Price Range ($)')
        axes[0, 0].set_ylabel('Number of Products')
        
        # Plot 2: Distribution of coefficient of variation
        cvs = price_stats[price_stats['count'] > 1]['cv']
        axes[0, 1].hist(cvs, bins=30, alpha=0.7, edgecolor='black', color='lightgreen')
        axes[0, 1].set_title('Distribution of Price Variation (CV)')
        axes[0, 1].set_xlabel('Coefficient of Variation')
        axes[0, 1].set_ylabel('Number of Products')
        
        # Plot 3: Price range vs mean price scatter
        multi_store = price_stats[price_stats['count'] > 1]
        axes[1, 0].scatter(multi_store['mean'], multi_store['price_range'], alpha=0.6, color='purple')
        axes[1, 0].set_title('Price Range vs Mean Price')
        axes[1, 0].set_xlabel('Mean Price ($)')
        axes[1, 0].set_ylabel('Price Range ($)')
        
        # Plot 4: Top 10 products by price range
        top_10 = price_stats.sort_values('price_range', ascending=False).head(10)
        y_pos = range(len(top_10))
        axes[1, 1].barh(y_pos, top_10['price_range'], color='orange')
        axes[1, 1].set_yticks(y_pos)
        axes[1, 1].set_yticklabels([p[:25] + '...' if len(p) > 25 else p for p in top_10.index])
        axes[1, 1].set_title('Top 10 Products by Price Range')
        axes[1, 1].set_xlabel('Price Range ($)')
        
        plt.tight_layout()
        plt.savefig('price_variance_analysis.png', dpi=300, bbox_inches='tight')
        print("Price variance visualization saved as 'price_variance_analysis.png'")
        
    except Exception as e:
        print(f"Error creating visualization: {e}")

def main():
    """Main function to run price variance analysis"""
    print("Trader Joe's Price Variance Analysis")
    print("=" * 60)
    
    # Load the data
    df = load_data()
    
    if df is None:
        print("Cannot proceed without data.")
        return
    
    # Run the analysis
    price_pivot, price_stats, multi_store_products = analyze_price_variance(df)
    
    if len(multi_store_products) == 0:
        print("No products found in multiple stores!")
        return
    
    # Find highest variance products
    top_by_range = find_highest_variance_products(price_stats, price_pivot)
    top_by_cv = find_highest_relative_variance(price_stats, price_pivot)
    
    # Analyze store patterns
    store_stats = analyze_store_price_patterns(price_pivot, price_stats)
    
    # Create visualizations
    create_variance_visualizations(price_stats, price_pivot)
    
    print("\n🎉 Price variance analysis complete!")
    print("Check 'price_variance_analysis.png' for visual insights.")

if __name__ == "__main__":
    import os
    main() 