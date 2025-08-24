# 🛒 Trader Joe's Large CSV Data Analysis Toolkit

This toolkit helps you efficiently analyze and practice data manipulation on large CSV files without running into memory issues.

## 🚀 Features

- **Memory-efficient processing** of large CSV files
- **Multiple analysis approaches** (Pandas chunks, Dask, Vaex)
- **Automatic method selection** based on file size
- **Sample dataset creation** for detailed analysis
- **Comprehensive practice exercises** for data manipulation

## 📦 Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Verify installation:**
   ```bash
   python -c "import pandas, dask.dataframe, vaex; print('✅ All packages installed successfully!')"
   ```

## 🎯 How to Use

### Step 1: Analyze Your Large CSV

Run the large CSV analyzer to get information about your file and create a manageable sample:

```bash
python large_csv_analyzer.py /path/to/your/trader_joes_data.csv
```

Or run interactively:
```bash
python large_csv_analyzer.py
# Then enter the file path when prompted
```

**What this does:**
- 📊 Shows file size and row count
- 🔍 Previews the first few lines
- ⚡ Automatically chooses the best analysis method
- 💾 Creates a sample dataset for further analysis

### Step 2: Practice Data Manipulation

Once you have a sample dataset, practice various data operations:

```bash
python data_practice.py
```

**Practice exercises include:**
- 🔍 **Basic Exploration**: Shape, columns, data types, missing values
- 🔍 **Filtering**: Filter data by specific conditions
- 📊 **Sorting**: Sort by different columns and criteria
- 📈 **Aggregation**: Group data and calculate statistics
- 🧹 **Data Cleaning**: Handle missing values and duplicates
- 📊 **Visualization**: Create charts and plots

## 🛠️ Analysis Methods

The toolkit automatically selects the best method based on your file size:

| File Size | Method | Best For |
|-----------|--------|-----------|
| < 100 MB | Pandas Chunks | Medium files, familiar syntax |
| 100 MB - 1 GB | Dask | Large files, parallel processing |
| > 1 GB | Vaex | Very large files, memory mapping |

## 📊 Sample Output

### File Information
```
📁 File: /path/to/trader_joes_data.csv
📏 Size: 2.45 GB
📊 Total lines: 15,234,567
```

### Data Preview
```
🔍 First 5 lines preview:
Line 1: product_name,price,category,store_location
Line 2: Organic Bananas,0.49,Produce,Store_001
Line 3: Greek Yogurt,4.99,Dairy,Store_002
...
```

### Sample Dataset
```
💾 Sample saved to: trader_joes_sample_1000.csv
📊 Sample shape: (1000, 8)
💾 Sample size: 156.78 KB
```

## 🎓 Learning Path

1. **Start with the analyzer** to understand your data structure
2. **Use the practice script** to learn basic operations
3. **Modify the scripts** to try your own analysis
4. **Explore the sample data** with pandas for detailed work

## 🔧 Customization

### Modify Sample Size
In `large_csv_analyzer.py`, change the `sample_size` parameter:
```python
analyzer.filter_and_sample(sample_size=5000)  # Create 5K row sample
```

### Add Your Own Analysis
In `data_practice.py`, add new functions and call them in `main()`:
```python
def my_custom_analysis(df):
    # Your custom analysis here
    pass

# Add to main():
my_custom_analysis(df)
```

## 🐛 Troubleshooting

### Common Issues

**"No module named 'vaex'"**
```bash
pip install vaex
```

**"Memory error"**
- The toolkit automatically handles this, but you can reduce chunk sizes
- Use smaller sample sizes

**"File not found"**
- Check the file path is correct
- Use absolute paths if needed

### Performance Tips

- **For very large files**: Use Vaex (automatic for >1GB files)
- **For medium files**: Dask provides good balance of speed and memory
- **For exploration**: Use the sample dataset with regular pandas

## 📚 Next Steps

After mastering the basics:
1. **Try different filtering conditions** on your data
2. **Create custom aggregations** specific to your analysis
3. **Build interactive dashboards** with the sample data
4. **Apply machine learning** techniques to the cleaned dataset

## 🤝 Contributing

Feel free to:
- Add new analysis methods
- Improve the practice exercises
- Share your custom analysis functions
- Report bugs or suggest improvements

---

**Happy data exploring! 🎉**

*This toolkit is designed to make large CSV analysis accessible and educational. Start with the analyzer, then dive into the practice exercises to build your data manipulation skills.* 