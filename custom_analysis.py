#!/usr/bin/env python3
"""
Custom Analysis Script for Trader Joe's Data
Demonstrates advanced pandas operations and interesting insights
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

def price_analysis(df):
    """Analyze price patterns"""
    print("\n💰 PRICE ANALYSIS")
    print("=" * 50)
    
    # Price distribution by store
    print("Price statistics by store:")
    price_by_store = df.groupby('store_code')['retail_price'].agg(['count', 'mean', 'min', 'max', 'std'])
    print(price_by_store.round(2))
    
    # Price ranges
    print(f"\nPrice ranges:")
    print(f"Cheapest item: ${df['retail_price'].min():.2f}")
    print(f"Most expensive item: ${df['retail_price'].max():.2f}")
    print(f"Average price: ${df['retail_price'].mean():.2f}")
    print(f"Median price: ${df['retail_price'].median():.2f}")
    
    # Price categories
    df['price_category'] = pd.cut(df['retail_price'], 
                                 bins=[0, 2, 5, 10, 25], 
                                 labels=['Budget ($0-2)', 'Affordable ($2-5)', 'Mid-range ($5-10)', 'Premium ($10+)'])
    
    print(f"\nPrice categories:")
    price_cats = df['price_category'].value_counts()
    print(price_cats)
    
    return df

def store_analysis(df):
    """Analyze store patterns"""
    print("\n🏪 STORE ANALYSIS")
    print("=" * 50)
    
    # Store statistics
    store_stats = df.groupby('store_code').agg({
        'retail_price': ['count', 'mean', 'sum'],
        'availability': 'mean'
    }).round(2)
    
    print("Store statistics:")
    print(store_stats)
    
    # Store comparison
    print(f"\nStore comparison:")
    for store in df['store_code'].unique():
        store_data = df[df['store_code'] == store]
        print(f"Store {store}: {len(store_data)} items, avg price: ${store_data['retail_price'].mean():.2f}, "
              f"availability: {store_data['availability'].mean()*100:.1f}%")
    
    return df

def product_analysis(df):
    """Analyze product patterns"""
    print("\n🛍️ PRODUCT ANALYSIS")
    print("=" * 50)
    
    # Most common products
    print("Top 10 most common products:")
    top_products = df['item_title'].value_counts().head(10)
    for i, (product, count) in enumerate(top_products.items(), 1):
        print(f"{i:2d}. {product}: {count} occurrences")
    
    # Product categories (simple keyword-based)
    keywords = ['organic', 'gluten', 'vegan', 'chocolate', 'coffee', 'sauce', 'bread', 'cheese']
    print(f"\nProduct categories by keywords:")
    for keyword in keywords:
        count = df['item_title'].str.contains(keyword, case=False, na=False).sum()
        print(f"{keyword.capitalize()}: {count} products")
    
    return df

def availability_analysis(df):
    """Analyze availability patterns"""
    print("\n📦 AVAILABILITY ANALYSIS")
    print("=" * 50)
    
    # Overall availability
    total_items = len(df)
    available_items = df['availability'].sum()
    unavailable_items = total_items - available_items
    
    print(f"Total items: {total_items}")
    print(f"Available: {available_items} ({available_items/total_items*100:.1f}%)")
    print(f"Unavailable: {unavailable_items} ({unavailable_items/total_items*100:.1f}%)")
    
    # Availability by price
    print(f"\nAvailability by price category:")
    df['price_category'] = pd.cut(df['retail_price'], 
                                 bins=[0, 2, 5, 10, 25], 
                                 labels=['Budget', 'Affordable', 'Mid-range', 'Premium'])
    
    avail_by_price = df.groupby('price_category')['availability'].agg(['count', 'sum', 'mean'])
    print(avail_by_price.round(3))
    
    return df

def create_advanced_visualizations(df):
    """Create more interesting visualizations"""
    print("\n📊 CREATING ADVANCED VISUALIZATIONS")
    print("=" * 50)
    
    try:
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create a figure with multiple subplots
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Advanced Trader Joe\'s Data Analysis', fontsize=16)
        
        # Plot 1: Price distribution by store
        df.boxplot(column='retail_price', by='store_code', ax=axes[0, 0])
        axes[0, 0].set_title('Price Distribution by Store')
        axes[0, 0].set_xlabel('Store Code')
        axes[0, 0].set_ylabel('Price ($)')
        
        # Plot 2: Availability by price category
        avail_by_price = df.groupby('price_category')['availability'].mean()
        avail_by_price.plot(kind='bar', ax=axes[0, 1], color='skyblue')
        axes[0, 1].set_title('Availability by Price Category')
        axes[0, 1].set_xlabel('Price Category')
        axes[0, 1].set_ylabel('Availability Rate')
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Plot 3: Price histogram
        axes[0, 2].hist(df['retail_price'], bins=30, alpha=0.7, edgecolor='black', color='lightgreen')
        axes[0, 2].set_title('Price Distribution')
        axes[0, 2].set_xlabel('Price ($)')
        axes[0, 2].set_ylabel('Frequency')
        
        # Plot 4: Store item counts
        store_counts = df['store_code'].value_counts().sort_index()
        store_counts.plot(kind='bar', ax=axes[1, 0], color='orange')
        axes[1, 0].set_title('Number of Items per Store')
        axes[1, 0].set_xlabel('Store Code')
        axes[1, 0].set_ylabel('Item Count')
        
        # Plot 5: Availability heatmap by store and price category
        pivot_table = df.pivot_table(values='availability', 
                                   index='price_category', 
                                   columns='store_code', 
                                   aggfunc='mean')
        sns.heatmap(pivot_table, annot=True, cmap='RdYlGn', center=0.5, ax=axes[1, 1])
        axes[1, 1].set_title('Availability Heatmap\n(Store vs Price Category)')
        
        # Plot 6: Price vs Availability scatter
        axes[1, 2].scatter(df['retail_price'], df['availability'], alpha=0.6, color='purple')
        axes[1, 2].set_title('Price vs Availability')
        axes[1, 2].set_xlabel('Price ($)')
        axes[1, 2].set_ylabel('Availability (0=No, 1=Yes)')
        
        plt.tight_layout()
        plt.savefig('advanced_trader_joes_analysis.png', dpi=300, bbox_inches='tight')
        print("Advanced visualization saved as 'advanced_trader_joes_analysis.png'")
        
    except Exception as e:
        print(f"Error creating visualization: {e}")

def main():
    """Main function to run custom analysis"""
    print("Trader Joe's Custom Data Analysis")
    print("=" * 60)
    
    # Load the data
    df = load_data()
    
    if df is None:
        print("Cannot proceed without data.")
        return
    
    # Run all analyses
    df = price_analysis(df)
    df = store_analysis(df)
    df = product_analysis(df)
    df = availability_analysis(df)
    create_advanced_visualizations(df)
    
    print("\n🎉 Custom analysis complete!")
    print("Check the generated visualization file for insights.")

if __name__ == "__main__":
    import os
    main() 