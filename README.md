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

data-engineering-intern-assignment/
├── README.md
├── config/
├── data/
│   ├── raw/
│   │   └── products.json
│   ├── silver/
│   └── gold/
├── docs/
├── sql/
└── src/
    └── explore_data.ipynb