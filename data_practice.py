#!/usr/bin/env python3
"""
Simple Data Manipulation Practice Script
Works with the sample CSV created by the large CSV analyzer
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def load_sample_data():
    """Load the sample data created by the large CSV analyzer"""
    try:
        # Try to find the sample file
        import glob
        sample_files = glob.glob("trader_joes_sample_*.csv")
        
        if sample_files:
            # Use the most recent sample file
            sample_file = max(sample_files, key=lambda x: os.path.getctime(x))
            print(f"Loading sample data from: {sample_file}")
            df = pd.read_csv(sample_file)
            return df
        else:
            print("No sample file found. Please run the large_csv_analyzer.py first.")
            return None
    except Exception as e:
        print(f"Error loading sample data: {e}")
        return None

def basic_exploration(df):
    """Basic data exploration"""
    print("\nBASIC DATA EXPLORATION")
    print("=" * 40)
    
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
    
    print(f"\nFirst 5 rows:")
    print(df.head())
    
    print(f"\nLast 5 rows:")
    print(df.tail())
    
    print(f"\nBasic statistics:")
    print(df.describe())
    
    print(f"\nMissing values:")
    print(df.isnull().sum())

def filtering_practice(df):
    """Practice filtering operations"""
    print("\nFILTERING PRACTICE")
    print("=" * 40)
    
    # Show unique values in first few columns
    for col in df.columns[:3]:  # First 3 columns
        if df[col].dtype == 'object':  # String/categorical columns
            unique_vals = df[col].value_counts().head(5)
            print(f"\nTop 5 values in '{col}':")
            print(unique_vals)
    
    # Demonstrate filtering
    print(f"\nFiltering examples:")
    
    # Filter by first column (assuming it's categorical)
    first_col = df.columns[0]
    if df[first_col].dtype == 'object':
        top_value = df[first_col].value_counts().index[0]
        filtered_df = df[df[first_col] == top_value]
        print(f"Rows where '{first_col}' equals '{top_value}': {len(filtered_df):,}")
        
        if len(filtered_df) > 0:
            print(f"Sample of filtered data:")
            print(filtered_df.head(3))

def sorting_practice(df):
    """Practice sorting operations"""
    print("\nSORTING PRACTICE")
    print("=" * 40)
    
    # Sort by first column
    first_col = df.columns[0]
    print(f"Sorting by '{first_col}' (ascending):")
    sorted_df = df.sort_values(first_col)
    print(sorted_df.head(5))
    
    # Sort by first column descending
    print(f"\nSorting by '{first_col}' (descending):")
    sorted_df_desc = df.sort_values(first_col, ascending=False)
    print(sorted_df_desc.head(5))
    
    # If there are numeric columns, sort by them
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        numeric_col = numeric_cols[0]
        print(f"\nSorting by numeric column '{numeric_col}' (descending):")
        sorted_numeric = df.sort_values(numeric_col, ascending=False)
        print(sorted_numeric.head(5))

def aggregation_practice(df):
    """Practice aggregation operations"""
    print("\nAGGREGATION PRACTICE")
    print("=" * 40)
    
    # Group by first categorical column
    first_col = df.columns[0]
    if df[first_col].dtype == 'object':
        print(f"Grouping by '{first_col}':")
        grouped = df.groupby(first_col)
        
        print(f"Number of groups: {len(grouped)}")
        
        # Show group sizes
        group_sizes = grouped.size()
        print(f"\nGroup sizes:")
        print(group_sizes.head(10))
        
        # If there are numeric columns, show aggregations
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            numeric_col = numeric_cols[0]
            print(f"\nAggregating '{numeric_col}' by '{first_col}':")
            agg_result = grouped[numeric_col].agg(['count', 'mean', 'min', 'max'])
            print(agg_result.head(10))

def data_cleaning_practice(df):
    """Practice data cleaning operations"""
    print("\nDATA CLEANING PRACTICE")
    print("=" * 40)
    
    # Check for missing values
    missing_counts = df.isnull().sum()
    print(f"Missing values per column:")
    print(missing_counts)
    
    # Handle missing values
    if missing_counts.sum() > 0:
        print(f"\nHandling missing values...")
        
        # Fill numeric columns with mean
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().sum() > 0:
                mean_val = df[col].mean()
                df[col].fillna(mean_val, inplace=True)
                print(f"Filled missing values in '{col}' with mean: {mean_val:.2f}")
        
        # Fill categorical columns with mode
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if df[col].isnull().sum() > 0:
                mode_val = df[col].mode()[0]
                df[col].fillna(mode_val, inplace=True)
                print(f"Filled missing values in '{col}' with mode: {mode_val}")
    
    # Remove duplicates
    initial_rows = len(df)
    df.drop_duplicates(inplace=True)
    final_rows = len(df)
    
    if initial_rows != final_rows:
        print(f"Removed {initial_rows - final_rows} duplicate rows")
    else:
        print("No duplicate rows found")
    
    print(f"Final dataset shape: {df.shape}")

def visualization_practice(df):
    """Practice creating visualizations"""
    print("\nVISUALIZATION PRACTICE")
    print("=" * 40)
    
    try:
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create a figure with multiple subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Trader Joe\'s Data Analysis', fontsize=16)
        
        # Plot 1: Distribution of first categorical column
        first_col = df.columns[0]
        if df[first_col].dtype == 'object':
            value_counts = df[first_col].value_counts().head(10)
            axes[0, 0].bar(range(len(value_counts)), value_counts.values)
            axes[0, 0].set_title(f'Top 10 values in {first_col}')
            axes[0, 0].set_xticks(range(len(value_counts)))
            axes[0, 0].set_xticklabels(value_counts.index, rotation=45, ha='right')
            axes[0, 0].set_ylabel('Count')
        
        # Plot 2: Distribution of first numeric column
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            numeric_col = numeric_cols[0]
            axes[0, 1].hist(df[numeric_col].dropna(), bins=20, alpha=0.7, edgecolor='black')
            axes[0, 1].set_title(f'Distribution of {numeric_col}')
            axes[0, 1].set_xlabel(numeric_col)
            axes[0, 1].set_ylabel('Frequency')
        
        # Plot 3: Missing values heatmap
        missing_data = df.isnull()
        if missing_data.sum().sum() > 0:
            sns.heatmap(missing_data, cbar=True, ax=axes[1, 0])
            axes[1, 0].set_title('Missing Values Heatmap')
        else:
            axes[1, 0].text(0.5, 0.5, 'No Missing Values', ha='center', va='center', transform=axes[1, 0].transAxes)
            axes[1, 0].set_title('Missing Values')
        
        # Plot 4: Correlation matrix (if multiple numeric columns)
        if len(numeric_cols) > 1:
            correlation_matrix = df[numeric_cols].corr()
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, ax=axes[1, 1])
            axes[1, 1].set_title('Correlation Matrix')
        else:
            axes[1, 1].text(0.5, 0.5, 'Need 2+ numeric columns\nfor correlation', ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Correlation Matrix')
        
        plt.tight_layout()
        plt.savefig('trader_joes_analysis.png', dpi=300, bbox_inches='tight')
        print("Visualization saved as 'trader_joes_analysis.png'")
        
    except Exception as e:
        print(f"Error creating visualization: {e}")

def main():
    """Main function to run the data practice"""
    print("Trader Joe's Data Manipulation Practice")
    print("=" * 50)
    
    # Load the sample data
    df = load_sample_data()
    
    if df is None:
        print("Cannot proceed without sample data.")
        print("Please run 'python large_csv_analyzer.py' first to create a sample.")
        return
    
    # Run all practice exercises
    basic_exploration(df)
    filtering_practice(df)
    sorting_practice(df)
    aggregation_practice(df)
    data_cleaning_practice(df)
    visualization_practice(df)
    
    print("\nPractice session complete!")
    print("You can now explore the data further or modify the scripts.")

if __name__ == "__main__":
    import os
    main() 