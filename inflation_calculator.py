#!/usr/bin/env python3
"""
Trader Joe's Inflation Calculator
Calculate inflation rates for custom baskets of goods across different stores and time periods
"""

import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
import os

class InflationCalculator:
    def __init__(self, root):
        self.root = root
        self.root.title("Trader Joe's Inflation Calculator")
        self.root.geometry("1000x800")
        
        # Load the core products data
        self.load_data()
        
        # Create the GUI
        self.create_widgets()
        
        # Initialize basket
        self.basket = []
        self.update_basket_display()
    
    def load_data(self):
        """Load the core products data"""
        try:
            csv_file = 'trader_joes_core_products.csv'
            if not os.path.exists(csv_file):
                messagebox.showerror("Error", f"Core products file not found: {csv_file}")
                return
            
            self.df = pd.read_csv(csv_file)
            print(f"✅ Loaded {len(self.df)} products")
            
            # Get unique store codes from column names
            store_columns = [col for col in self.df.columns if col.startswith('store_') and '_first_price' in col]
            self.store_codes = [col.split('_')[1] for col in store_columns]
            print(f"✅ Found stores: {self.store_codes}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data: {e}")
            self.df = None
            self.store_codes = []
    
    def create_widgets(self):
        """Create the GUI widgets"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Store selection
        ttk.Label(main_frame, text="Select Store:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.store_var = tk.StringVar()
        self.store_combo = ttk.Combobox(main_frame, textvariable=self.store_var, values=self.store_codes, state="readonly")
        self.store_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5)
        self.store_combo.bind('<<ComboboxSelected>>', self.on_store_change)
        
        if self.store_codes:
            self.store_combo.set(self.store_codes[0])
        
        # Search frame
        search_frame = ttk.LabelFrame(main_frame, text="Search Products", padding="10")
        search_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)
        search_frame.columnconfigure(1, weight=1)
        
        ttk.Label(search_frame, text="Search:").grid(row=0, column=0, sticky=tk.W)
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        self.search_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(10, 0))
        self.search_entry.bind('<KeyRelease>', self.on_search)
        
        # Product list
        ttk.Label(search_frame, text="Products:").grid(row=1, column=0, sticky=tk.W, pady=(10, 0))
        self.product_listbox = tk.Listbox(search_frame, height=8)
        self.product_listbox.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0))
        self.product_listbox.bind('<Double-Button-1>', self.add_to_basket)
        
        # Add to basket button
        ttk.Button(search_frame, text="Add to Basket", command=self.add_selected_to_basket).grid(row=3, column=0, columnspan=2, pady=10)
        
        # Basket frame
        basket_frame = ttk.LabelFrame(main_frame, text="Your Basket", padding="10")
        basket_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)
        basket_frame.columnconfigure(0, weight=1)
        
        # Basket display
        self.basket_text = tk.Text(basket_frame, height=6, wrap=tk.WORD)
        self.basket_text.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Basket controls
        button_frame = ttk.Frame(basket_frame)
        button_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))
        button_frame.columnconfigure(1, weight=1)
        
        ttk.Button(button_frame, text="Clear Basket", command=self.clear_basket).grid(row=0, column=0, padx=(0, 10))
        ttk.Button(button_frame, text="Remove Selected", command=self.remove_from_basket).grid(row=0, column=1, padx=(0, 10))
        ttk.Button(button_frame, text="Calculate Inflation", command=self.calculate_inflation).grid(row=0, column=2)
        
        # Results frame
        results_frame = ttk.LabelFrame(main_frame, text="Inflation Results", padding="10")
        results_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)
        results_frame.columnconfigure(0, weight=1)
        
        self.results_text = tk.Text(results_frame, height=8, wrap=tk.WORD)
        self.results_text.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # Initialize product list
        self.update_product_list()
    
    def on_store_change(self, event=None):
        """Handle store selection change"""
        self.update_product_list()
        self.update_basket_display()
    
    def on_search(self, event=None):
        """Handle search input"""
        self.update_product_list()
    
    def update_product_list(self):
        """Update the product list based on search and store selection"""
        if self.df is None:
            return
        
        store = self.store_var.get()
        search_term = self.search_var.get().lower()
        
        # Filter products
        if search_term:
            filtered_df = self.df[self.df['item_title'].str.lower().str.contains(search_term, na=False)]
        else:
            filtered_df = self.df
        
        # Get products available at selected store
        first_price_col = f'store_{store}_first_price'
        last_price_col = f'store_{store}_last_price'
        
        available_products = filtered_df[
            (filtered_df[first_price_col] > 0) & 
            (filtered_df[last_price_col] > 0)
        ]
        
        # Update listbox
        self.product_listbox.delete(0, tk.END)
        for _, row in available_products.iterrows():
            first_price = row[first_price_col]
            last_price = row[last_price_col]
            price_change = last_price - first_price
            price_change_pct = (price_change / first_price * 100) if first_price > 0 else 0
            
            display_text = f"{row['item_title'][:50]}... | ${first_price:.2f} → ${last_price:.2f} ({price_change_pct:+.1f}%)"
            self.product_listbox.insert(tk.END, display_text)
            
            # Store the actual item data
            self.product_listbox.itemconfig(tk.END, {'data': row})
    
    def add_to_basket(self, event):
        """Add double-clicked item to basket"""
        selection = self.product_listbox.curselection()
        if selection:
            self.add_selected_to_basket()
    
    def add_selected_to_basket(self):
        """Add selected item to basket"""
        selection = self.product_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a product first")
            return
        
        store = self.store_var.get()
        if not store:
            messagebox.showwarning("Warning", "Please select a store first")
            return
        
        # Get the selected item data
        item_data = self.product_listbox.itemconfig(selection[0], 'data')[4]  # Get the data attribute
        
        # Check if item is already in basket
        item_title = item_data['item_title']
        if any(item['title'] == item_title for item in self.basket):
            messagebox.showwarning("Warning", f"{item_title} is already in your basket")
            return
        
        # Add to basket
        basket_item = {
            'title': item_title,
            'first_price': item_data[f'store_{store}_first_price'],
            'last_price': item_data[f'store_{store}_last_price'],
            'price_change': item_data[f'store_{store}_price_change'],
            'price_change_percent': item_data[f'store_{store}_price_change_percent'],
            'first_date': item_data['first_appearance'],
            'last_date': item_data['last_appearance'],
            'duration_days': item_data['duration_days']
        }
        
        self.basket.append(basket_item)
        self.update_basket_display()
    
    def remove_from_basket(self):
        """Remove selected item from basket"""
        # For simplicity, we'll remove the last item added
        # In a more sophisticated version, you could add a selection mechanism
        if self.basket:
            removed_item = self.basket.pop()
            self.update_basket_display()
            messagebox.showinfo("Info", f"Removed: {removed_item['title']}")
        else:
            messagebox.showwarning("Warning", "Basket is empty")
    
    def clear_basket(self):
        """Clear the entire basket"""
        self.basket.clear()
        self.update_basket_display()
        self.results_text.delete(1.0, tk.END)
    
    def update_basket_display(self):
        """Update the basket display"""
        self.basket_text.delete(1.0, tk.END)
        
        if not self.basket:
            self.basket_text.insert(tk.END, "Your basket is empty. Add some products to get started!")
            return
        
        store = self.store_var.get()
        self.basket_text.insert(tk.END, f"Basket for Store {store}:\n")
        self.basket_text.insert(tk.END, "=" * 50 + "\n\n")
        
        total_first_value = 0
        total_last_value = 0
        
        for i, item in enumerate(self.basket, 1):
            self.basket_text.insert(tk.END, f"{i}. {item['title']}\n")
            self.basket_text.insert(tk.END, f"   First Price: ${item['first_price']:.2f} ({item['first_date']})\n")
            self.basket_text.insert(tk.END, f"   Last Price:  ${item['last_price']:.2f} ({item['last_date']})\n")
            self.basket_text.insert(tk.END, f"   Change:      ${item['price_change']:+.2f} ({item['price_change_percent']:+.1f}%)\n")
            self.basket_text.insert(tk.END, f"   Period:      {item['duration_days']} days\n\n")
            
            total_first_value += item['first_price']
            total_last_value += item['last_price']
        
        self.basket_text.insert(tk.END, "=" * 50 + "\n")
        self.basket_text.insert(tk.END, f"Total Basket Value:\n")
        self.basket_text.insert(tk.END, f"First Period: ${total_first_value:.2f}\n")
        self.basket_text.insert(tk.END, f"Last Period:  ${total_last_value:.2f}\n")
        self.basket_text.insert(tk.END, f"Total Change: ${total_last_value - total_first_value:+.2f}\n")
    
    def calculate_inflation(self):
        """Calculate weighted inflation for the basket"""
        if not self.basket:
            messagebox.showwarning("Warning", "Please add products to your basket first")
            return
        
        store = self.store_var.get()
        if not store:
            messagebox.showwarning("Warning", "Please select a store first")
            return
        
        # Calculate weighted inflation
        total_first_value = sum(item['first_price'] for item in self.basket)
        total_last_value = sum(item['last_price'] for item in self.basket)
        
        # Calculate weighted average price change
        weighted_price_changes = []
        total_weight = 0
        
        for item in self.basket:
            weight = item['first_price']  # Weight by initial price
            total_weight += weight
            weighted_price_changes.append(item['price_change_percent'] * weight)
        
        weighted_avg_change = sum(weighted_price_changes) / total_weight if total_weight > 0 else 0
        
        # Calculate time-weighted inflation (annualized)
        avg_duration_days = sum(item['duration_days'] for item in self.basket) / len(self.basket)
        annualized_inflation = weighted_avg_change * (365 / avg_duration_days) if avg_duration_days > 0 else 0
        
        # Display results
        self.results_text.delete(1.0, tk.END)
        
        self.results_text.insert(tk.END, f"INFLATION ANALYSIS FOR STORE {store}\n")
        self.results_text.insert(tk.END, "=" * 50 + "\n\n")
        
        self.results_text.insert(tk.END, f"Basket Composition:\n")
        self.results_text.insert(tk.END, f"Number of items: {len(self.basket)}\n")
        self.results_text.insert(tk.END, f"Total initial value: ${total_first_value:.2f}\n")
        self.results_text.insert(tk.END, f"Total final value: ${total_last_value:.2f}\n\n")
        
        self.results_text.insert(tk.END, f"Price Changes:\n")
        self.results_text.insert(tk.END, f"Total basket change: ${total_last_value - total_first_value:+.2f}\n")
        self.results_text.insert(tk.END, f"Percentage change: {((total_last_value - total_first_value) / total_first_value * 100):+.2f}%\n\n")
        
        self.results_text.insert(tk.END, f"Weighted Analysis:\n")
        self.results_text.insert(tk.END, f"Average duration: {avg_duration_days:.1f} days\n")
        self.results_text.insert(tk.END, f"Weighted average price change: {weighted_avg_change:+.2f}%\n")
        self.results_text.insert(tk.END, f"Annualized inflation rate: {annualized_inflation:+.2f}%\n\n")
        
        # Categorize inflation
        if annualized_inflation > 2:
            category = "HIGH INFLATION"
        elif annualized_inflation > 0:
            category = "MODERATE INFLATION"
        elif annualized_inflation > -2:
            category = "PRICE STABILITY"
        else:
            category = "DEFLATION"
        
        self.results_text.insert(tk.END, f"Category: {category}\n")
        
        # Individual item analysis
        self.results_text.insert(tk.END, f"\nIndividual Item Analysis:\n")
        self.results_text.insert(tk.END, "-" * 30 + "\n")
        
        for i, item in enumerate(self.basket, 1):
            weight_pct = (item['first_price'] / total_first_value * 100) if total_first_value > 0 else 0
            self.results_text.insert(tk.END, f"{i}. {item['title'][:30]}...\n")
            self.results_text.insert(tk.END, f"   Weight: {weight_pct:.1f}% | Change: {item['price_change_percent']:+.1f}%\n")

def main():
    """Main function"""
    root = tk.Tk()
    app = InflationCalculator(root)
    root.mainloop()

if __name__ == "__main__":
    main() 