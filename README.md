# Data Engineering Internship Assignment

## 1. Project Overview

This project implements an end-to-end ETL pipeline using PySpark with a Raw → Silver → Gold architecture.

The source data is an e-commerce product dataset obtained from the DummyJSON public API.

## 2. Source Information

- Source: DummyJSON Products API
- Source URL: https://dummyjson.com/products
- API request used: https://dummyjson.com/products?limit=0
- Source Type: Public REST API
- Data Format: JSON
- Download Date: 2026-09-13

## 3. Source Description

DummyJSON provides simulated e-commerce product data.

The Products API returned 194 products.

Each product contains product information such as:

- Product ID
- Title
- Description
- Category
- Price
- Discount Percentage
- Rating
- Stock
- Tags
- Brand
- SKU
- Weight
- Dimensions
- Warranty Information
- Shipping Information
- Availability Status
- Reviews
- Return Policy
- Minimum Order Quantity
- Metadata
- Images
- Thumbnail

## 4. Raw Data Structure

The raw JSON contains the following top-level structure:

- `products` → array containing product records
- `total` → total number of products
- `skip` → pagination offset
- `limit` → number of records returned

Product records contain both simple fields and nested structures.

Nested/array fields include:

- `tags` → array
- `dimensions` → nested object containing width, height and depth
- `reviews` → array of review objects
- `meta` → nested object containing createdAt, updatedAt, barcode and qrCode
- `images` → array

 ## 5. Project Structure

```text
data-engineering-intern-assignment/
├── README.md
├── data/
│   ├── raw/
│   │   └── products.json
│   ├── silver/
│   │   └── products/
│   └── gold/
│       ├── gold_product_kpi/
│       ├── gold_category_kpi/
│       ├── gold_brand_kpi/
│       └── gold_review_kpi/
├── docs/
│   └── data_quality_report.md
├── sql/
└── src/
    ├── explore_data.ipynb
    ├── raw_to_silver.py
    ├── data_quality.py
    └── silver_to_gold.py
```

  ## 6. Gold Layer

The Gold layer contains analytical KPI tables created from the Silver layer using PySpark.

The KPI requirements provided for the assignment were implemented as follows:

1. **Inventory Value**
   - Formula: `SUM(price × stock)`
   - Calculated at product, category, and overall levels.

2. **Discounted Inventory Value**
   - Formula: `SUM(price × (1 - discountPercentage / 100) × stock)`
   - Calculated at product and category levels.

3. **Category Sales Potential**
   - Formula: `SUM(price × stock)` grouped by category.
   - This represents estimated sales potential and not actual revenue because the dataset does not contain transaction or order data.

4. **Low Stock Rate**
   - Formula: `Low-stock products / Total products × 100`
   - Low-stock threshold: `stock <= 10`.

5. **Weighted Average Product Rating**
   - Formula: `SUM(rating × stock) / SUM(stock)`.

6. **Brand Discount Impact**
   - Calculates original inventory value, discount amount, discounted inventory value, average discount, and average rating by brand.

7. **Inventory Concentration**
   - Formula: `Category Inventory Value / Total Inventory Value × 100`.

8. **Product Health Score**
   - Formula:
     - 40% Rating Score
     - 25% Review Score
     - 20% Stock Score
     - 15% Discount Score
   - Components were normalized to a common 0–1 scale.

9. **Review Quality Index**
   - Positive Review % is calculated using 4-star and 5-star reviews.
   - Review-level metrics include total reviews, average review rating, and rating distribution.

10. **Commercial Opportunity Score**
    - Formula:
      - 30% Rating
      - 25% Review Quality
      - 20% Discount
      - 15% Stock Opportunity
      - 10% Price Score
    - Components were normalized to a common 0–1 scale.

### 7. Gold Tables

The following Gold Parquet tables were created:

| Gold Table | Records |
|---|---:|
| `gold_product_kpi` | 194 |
| `gold_category_kpi` | 24 |
| `gold_brand_kpi` | 64 |
| `gold_review_kpi` | 194 |

All Gold tables were validated by reading the generated Parquet files and checking their record counts.

## 8. Data Exploration

Initial dataset exploration was performed using Pandas in:

`src/explore_data.ipynb`

The exploration included:

- Dataset structure and dimensions
- Column names and data types
- Missing value analysis
- Duplicate product ID checks
- Numeric range validation
- Category distribution
- Brand distribution
- Availability status
- Return policy
- Nested fields and arrays
- Review analysis
- Dimensions and metadata
- Timestamp validation
- Product consistency checks

The dataset contains nested structures such as `reviews`, `dimensions`, `meta`, `tags`, and `images`.

## 9. Silver Layer

The Silver layer transformation is implemented in:

`src/raw_to_silver.py`

The Raw JSON data is read using PySpark and transformed into a structured Silver dataset.

The main transformations include:

- Exploding the `products` array
- Flattening product structures
- Extracting `width`, `height`, and `depth` from `dimensions`
- Extracting `barcode`, `created_at`, and `updated_at` from `meta`
- Standardizing column names to lowercase
- Removing the original `dimensions` and `meta` structures after extraction
- Converting timestamp fields to Spark timestamp data types
- Performing data quality checks
- Writing the Silver dataset to Parquet

Silver data is stored at:

`data/silver/products/`

## 10. Data Quality

Data quality checks were performed on the Silver layer using PySpark.

The checks included:

- Null / missing value checks
- Duplicate product ID checks
- Invalid price validation
- Invalid rating validation
- Invalid stock validation
- Invalid discount percentage validation
- Invalid minimum order quantity validation
- Invalid weight validation
- Timestamp validation
- Availability status validation
- Return policy validation
- Availability and stock consistency validation

### 11. Data Quality Results

| Check | Issue Count |
|---|---:|
| Missing brand values | 92 |
| Duplicate product IDs | 0 |
| Invalid price | 0 |
| Invalid rating | 0 |
| Invalid stock | 0 |
| Invalid discount percentage | 0 |
| Invalid minimum order quantity | 0 |
| Invalid weight | 0 |
| Invalid timestamps | 0 |
| Unexpected availability status | 0 |
| Unexpected return policy | 0 |
| Availability-stock inconsistency | 0 |

The only missing values identified were in the `brand` column.

- Total Silver records: 194
- Missing brand values: 92
- Records with brand present: 102

The missing brand values were preserved rather than artificially replacing them.

The detailed data quality report is available at:

`docs/data_quality_report.md`

## 12. How to Run

### Step 1: Create Virtual Environment

```bash
python -m venv .venv
```

### Step 2: Activate Virtual Environment

Windows:

```bash
.venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Run Raw to Silver Pipeline

```bash
python src/raw_to_silver.py
```

### Step 5: Run Data Quality Checks

```bash
python src/data_quality.py
```

### Step 6: Run Silver to Gold Pipeline

```bash
python src/silver_to_gold.py
```

The processed Silver and Gold datasets are written as Parquet files.

## 13. Technologies Used

- Python 3.11
- Apache Spark
- PySpark 3.5.7
- Pandas
- Requests
- JSON
- Parquet
- Jupyter Notebook
- VS Code

## 14. Final Pipeline Status

| Pipeline Stage | Status |
|---|---|
| Raw Layer | ✅ Complete |
| Data Exploration | ✅ Complete |
| Silver Transformation | ✅ Complete |
| Data Quality Checks | ✅ Complete |
| Gold KPI Development | ✅ Complete |
| Gold Parquet Output | ✅ Complete |
| Final Gold Validation | ✅ Complete |

### 15. Final Gold Validation

```text
Gold Product KPI: 194
Gold Category KPI: 24
Gold Brand KPI: 64
Gold Review KPI: 194

