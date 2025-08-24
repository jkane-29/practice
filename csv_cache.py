#!/usr/bin/env python3
"""
CSV Caching System for Trader Joe's Data
Stores processed data in memory and on disk for faster repeated access
"""

import pandas as pd
import dask.dataframe as dd
import os
import pickle
import hashlib
import time
from pathlib import Path
import json

class CSVCache:
    def __init__(self, cache_dir="cache"):
        """Initialize the CSV cache system"""
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.memory_cache = {}
        self.cache_metadata_file = self.cache_dir / "cache_metadata.json"
        self.load_cache_metadata()
    
    def load_cache_metadata(self):
        """Load cache metadata from disk"""
        if self.cache_metadata_file.exists():
            try:
                with open(self.cache_metadata_file, 'r') as f:
                    self.cache_metadata = json.load(f)
            except:
                self.cache_metadata = {}
        else:
            self.cache_metadata = {}
    
    def save_cache_metadata(self):
        """Save cache metadata to disk"""
        with open(self.cache_metadata_file, 'w') as f:
            json.dump(self.cache_metadata, f, indent=2)
    
    def get_file_hash(self, file_path):
        """Generate a hash for the file to detect changes"""
        try:
            # Get file size and modification time for quick change detection
            stat = os.stat(file_path)
            file_info = f"{file_path}_{stat.st_size}_{stat.st_mtime}"
            return hashlib.md5(file_info.encode()).hexdigest()
        except:
            return None
    
    def get_cache_key(self, file_path, operation, **kwargs):
        """Generate a cache key for the operation"""
        file_hash = self.get_file_hash(file_path)
        if not file_hash:
            return None
        
        # Create a unique key based on file hash, operation, and parameters
        key_parts = [file_hash, operation]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        
        return "_".join(key_parts)
    
    def get_cache_path(self, cache_key):
        """Get the file path for a cache key"""
        return self.cache_dir / f"{cache_key}.pkl"
    
    def is_cache_valid(self, cache_key, max_age_hours=24):
        """Check if cached data is still valid"""
        if cache_key not in self.cache_metadata:
            return False
        
        cache_info = self.cache_metadata[cache_key]
        cache_time = cache_info.get('timestamp', 0)
        current_time = time.time()
        
        # Check if cache is too old
        if current_time - cache_time > (max_age_hours * 3600):
            return False
        
        return True
    
    def get_cached_data(self, cache_key):
        """Retrieve cached data from memory or disk"""
        # Check memory cache first
        if cache_key in self.memory_cache:
            print(f"Using memory cache for: {cache_key[:20]}...")
            return self.memory_cache[cache_key]
        
        # Check disk cache
        cache_path = self.get_cache_path(cache_key)
        if cache_path.exists() and self.is_cache_valid(cache_key):
            try:
                print(f"Loading from disk cache: {cache_path}")
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                
                # Store in memory cache for faster access
                self.memory_cache[cache_key] = data
                return data
            except Exception as e:
                print(f"Error loading cache: {e}")
                return None
        
        return None
    
    def cache_data(self, cache_key, data, operation_info=None):
        """Cache data in memory and on disk"""
        # Store in memory cache
        self.memory_cache[cache_key] = data
        
        # Store on disk
        cache_path = self.get_cache_path(cache_key)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
            
            # Update metadata
            self.cache_metadata[cache_key] = {
                'timestamp': time.time(),
                'operation': operation_info or 'unknown',
                'size_bytes': os.path.getsize(cache_path),
                'data_type': type(data).__name__
            }
            self.save_cache_metadata()
            
            print(f"Data cached successfully: {cache_path}")
            return True
        except Exception as e:
            print(f"Error caching data: {e}")
            return False
    
    def cache_csv_data(self, csv_path, operation="load", **kwargs):
        """Cache CSV data with automatic key generation"""
        cache_key = self.get_cache_key(csv_path, operation, **kwargs)
        if not cache_key:
            return None, None
        
        # Check if we have valid cached data
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data, cache_key
        
        return None, cache_key
    
    def load_csv_with_cache(self, csv_path, method="dask", **kwargs):
        """Load CSV data with caching support"""
        print(f"Loading CSV with caching: {csv_path}")
        
        # Try to get from cache first
        cached_data, cache_key = self.cache_csv_data(
            csv_path, 
            operation="load", 
            method=method, 
            **kwargs
        )
        
        if cached_data is not None:
            print("Using cached data")
            return cached_data
        
        # Load fresh data
        print("Loading fresh data...")
        try:
            if method == "dask":
                data = dd.read_csv(csv_path, **kwargs)
            elif method == "pandas":
                data = pd.read_csv(csv_path, **kwargs)
            else:
                raise ValueError(f"Unknown method: {method}")
            
            # Cache the data
            self.cache_data(cache_key, data, f"load_{method}")
            return data
            
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return None
    
    def filter_data_with_cache(self, csv_path, filters, **kwargs):
        """Filter CSV data with caching support"""
        print(f"Filtering data with caching: {csv_path}")
        
        # Create a cache key based on filters
        filter_str = str(sorted(filters.items()))
        cached_data, cache_key = self.cache_csv_data(
            csv_path, 
            operation="filter", 
            filters=filter_str,
            **kwargs
        )
        
        if cached_data is not None:
            print("Using cached filtered data")
            return cached_data
        
        # Load and filter fresh data
        print("Loading and filtering fresh data...")
        try:
            # Load the data
            ddf = dd.read_csv(csv_path, **kwargs)
            
            # Apply filters
            for column, value in filters.items():
                if isinstance(value, str):
                    ddf = ddf[ddf[column].str.contains(value, case=False, na=False)]
                else:
                    ddf = ddf[ddf[column] == value]
            
            # Compute the filtered data
            filtered_data = ddf.compute()
            
            # Cache the filtered data
            self.cache_data(cache_key, filtered_data, f"filter_{len(filters)}_conditions")
            return filtered_data
            
        except Exception as e:
            print(f"Error filtering data: {e}")
            return None
    
    def get_cache_stats(self):
        """Get statistics about the cache"""
        total_files = len(self.cache_metadata)
        total_size = sum(info.get('size_bytes', 0) for info in self.cache_metadata.values())
        memory_entries = len(self.memory_cache)
        
        print(f"Cache Statistics:")
        print(f"  Total cached files: {total_files}")
        print(f"  Total cache size: {total_size / (1024*1024):.2f} MB")
        print(f"  Memory cache entries: {memory_entries}")
        
        # Show recent cache entries
        recent_entries = sorted(
            self.cache_metadata.items(), 
            key=lambda x: x[1].get('timestamp', 0), 
            reverse=True
        )[:5]
        
        print(f"\nRecent cache entries:")
        for key, info in recent_entries:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(info.get('timestamp', 0)))
            size_mb = info.get('size_bytes', 0) / (1024*1024)
            print(f"  {key[:30]}... | {timestamp} | {size_mb:.2f} MB")
    
    def clear_cache(self, cache_key=None):
        """Clear specific cache entry or entire cache"""
        if cache_key:
            # Clear specific entry
            if cache_key in self.memory_cache:
                del self.memory_cache[cache_key]
            
            cache_path = self.get_cache_path(cache_key)
            if cache_path.exists():
                cache_path.unlink()
            
            if cache_key in self.cache_metadata:
                del self.cache_metadata[cache_key]
                self.save_cache_metadata()
            
            print(f"Cleared cache entry: {cache_key}")
        else:
            # Clear entire cache
            self.memory_cache.clear()
            
            for cache_file in self.cache_dir.glob("*.pkl"):
                cache_file.unlink()
            
            self.cache_metadata.clear()
            self.save_cache_metadata()
            
            print("Cleared entire cache")

def main():
    """Demo the caching system"""
    print("CSV Caching System Demo")
    print("=" * 40)
    
    # Initialize cache
    cache = CSVCache()
    
    # Example usage
    csv_path = "/Users/kaner/Downloads/traderjoes-dump-3.csv"
    
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return
    
    # Load data with caching
    print("\n1. Loading CSV data with caching...")
    data = cache.load_csv_with_cache(csv_path, method="dask")
    
    if data is not None:
        print(f"Data loaded successfully: {type(data)}")
        
        # Filter data with caching
        print("\n2. Filtering data with caching...")
        filters = {'item_title': 'Tortilla Española', 'store_code': 701}
        filtered_data = cache.filter_data_with_cache(csv_path, filters)
        
        if filtered_data is not None:
            print(f"Filtered data shape: {filtered_data.shape}")
    
    # Show cache statistics
    print("\n3. Cache statistics:")
    cache.get_cache_stats()

if __name__ == "__main__":
    main() 