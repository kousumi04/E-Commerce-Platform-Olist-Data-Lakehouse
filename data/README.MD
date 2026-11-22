# Olist E-Commerce Dataset Setup

Raw, processed, and curated data files are intentionally excluded from Git via `.gitignore`.

## Download Instructions
1. Visit the official Kaggle dataset page:
   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Download and unzip the archive.
3. Place all **9 CSV files** directly inside `data/raw/`:
   - `olist_customers_dataset.csv`
   - `olist_geolocation_dataset.csv`
   - `olist_order_items_dataset.csv`
   - `olist_order_payments_dataset.csv`
   - `olist_order_reviews_dataset.csv`
   - `olist_orders_dataset.csv`
   - `olist_products_dataset.csv`
   - `olist_sellers_dataset.csv`
   - `product_category_name_translation.csv`

---

### Step 4: Explain
* **`.gitignore`** blocks `.env`, `.aws/`, credentials, and all `.csv`/`.parquet` files so you never accidentally push secrets or 120 MB of raw data to GitHub.
* **`.env.example`** documents every environment variable our pipeline will use. You will copy it to `.env` locally.
* **`requirements.txt`** pins `pyspark==3.5.9` (the 3.5 LTS line, which is rock-solid on Windows with Java 11/17; PySpark 4.3.0 is the newest major release if you ever want to upgrade later), `pandas==3.1.1`, and `boto3==1.44.6` for reproducible installs.

---

### Step 5: Verify

Run these commands in your activated `.venv` PowerShell terminal:

```powershell
# 1. Create your local .env from the template
Copy-Item .env.example .env

# 2. Install Python dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 3. Verify core imports
python -c "import pyspark, pandas, boto3, pytest; print('PySpark:', pyspark.__version__, '| Pandas:', pandas.__version__, '| Boto3:', boto3.__version__)"