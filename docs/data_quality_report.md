# Data Quality Report

## 1. Overview

This report documents the data quality checks performed on the Silver layer of the DummyJSON products dataset.

The Silver layer contains 194 product records after transforming the raw JSON data into a structured Parquet dataset.

## 2. Data Quality Checks

The following checks were performed:

- Null / missing value checks
- Duplicate product ID checks
- Numeric range validation
- Discount percentage validation
- Minimum order quantity validation
- Weight validation
- Timestamp validation
- Availability status validation
- Return policy validation
- Availability and stock consistency validation

## 3. Data Quality Results

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

## 4. Missing Values

The only missing values identified in the Silver dataset were in the `brand` column.

- Total Silver records: 194
- Records with missing brand: 92
- Records with brand present: 102

The missing brand values were preserved rather than artificially replaced.

## 5. Duplicate Check

Product IDs were checked for duplicates.

- Duplicate product IDs: 0

Therefore, no duplicate product records were identified using the product ID.

## 6. Numeric Validation

The following numeric fields were validated:

- `price`
- `rating`
- `stock`
- `discountpercentage`
- `minimumorderquantity`
- `weight`

No invalid numeric values were identified.

## 7. Timestamp Validation

The `created_at` and `updated_at` fields were converted to timestamp data types.

The validation checked whether `updated_at` occurred before `created_at`.

- Invalid timestamp records: 0

## 8. Categorical Validation

The following categorical fields were validated:

- `availabilitystatus`
- `returnpolicy`

No unexpected values were identified.

## 9. Consistency Validation

Availability status was checked against stock quantity.

Examples of invalid conditions checked:

- `Out of Stock` with stock greater than 0
- `In Stock` with stock less than or equal to 0

- Inconsistent records: 0

## 10. Conclusion

The Silver layer contains 194 product records.

The data quality checks identified 92 missing brand values. No duplicate product IDs, invalid numeric values, invalid timestamps, unexpected categorical values, or availability-stock inconsistencies were identified.

The Silver dataset was therefore used as the source for the Gold KPI layer.