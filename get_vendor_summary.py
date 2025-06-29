import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    filename="logs/get_vendor_summary.log",  
    filemode='a'          
)

def create_vendor_summary(conn):
    '''this func will merge the different table to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
WITH FreightSummary AS (
    SELECT VendorNumber, SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),

purchaseSummary AS (
    SELECT 
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.PurchasePrice,
        pp.Volume,
        pp.Price AS ACTUALPrice,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.PurchasePrice, pp.Volume, pp.Price
),

salesSummary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
)

SELECT 
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.ACTUALPrice,
    ps.Volume,
    ps.PurchasePrice,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalSalesQuantity,
    ss.TotalExciseTax,
    fs.FreightCost
FROM purchaseSummary ps
LEFT JOIN salesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs
    ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC;
""", conn)

return vendor_sales_summary

def clean_data(df):
    '''func will clean data '''
    df['VendorName'] = df['VendorName'].str.strip()
    df.fillna(0, inplace = True)
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = df['GrossProfit'] / df['TotalSalesDollars']
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

return df

if __name__ == '__main__':
    # Establish database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Generating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Performing Data Cleaning...')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Starting Data Ingestion...')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Ingestion complete.')
