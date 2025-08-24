#!/usr/bin/env python3
"""
Cache Management Script for Trader Joe's CSV Data
Simple interface to manage the caching system
"""

from csv_cache import CSVCache
import os

def main():
    """Main cache management interface"""
    print("CSV Cache Management")
    print("=" * 30)
    
    # Initialize cache
    cache = CSVCache()
    
    while True:
        print("\nOptions:")
        print("1. Show cache statistics")
        print("2. Clear entire cache")
        print("3. Test caching with your CSV")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            print("\n" + "=" * 30)
            cache.get_cache_stats()
            
        elif choice == "2":
            confirm = input("Are you sure you want to clear the entire cache? (y/n): ").strip().lower()
            if confirm == 'y':
                cache.clear_cache()
                print("Cache cleared successfully!")
            else:
                print("Cache clear cancelled.")
                
        elif choice == "3":
            csv_path = input("Enter the path to your CSV file: ").strip()
            
            if not os.path.exists(csv_path):
                print(f"File not found: {csv_path}")
                continue
            
            print(f"\nTesting caching with: {csv_path}")
            
            # Test loading with cache
            print("\n1. First load (should be slow):")
            data1 = cache.load_csv_with_cache(csv_path, method="dask")
            
            if data1 is not None:
                print(f"Data loaded: {type(data1)}")
                
                # Test filtering with cache
                print("\n2. Testing filtered data caching:")
                filters = {'store_code': 701}
                filtered_data = cache.filter_data_with_cache(csv_path, filters)
                
                if filtered_data is not None:
                    print(f"Filtered data shape: {filtered_data.shape}")
                
                # Test second load (should be fast)
                print("\n3. Second load (should be fast from cache):")
                data2 = cache.load_csv_with_cache(csv_path, method="dask")
                
                if data2 is not None:
                    print(f"Data loaded from cache: {type(data2)}")
                
                # Show final cache stats
                print("\n4. Final cache statistics:")
                cache.get_cache_stats()
            else:
                print("Failed to load data")
                
        elif choice == "4":
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice. Please enter 1-4.")

if __name__ == "__main__":
    main() 