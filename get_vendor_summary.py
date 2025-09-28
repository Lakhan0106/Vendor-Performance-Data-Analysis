import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

# Set up logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """
    This function merges different tables to create the overall vendor summary,
    including freight, purchase, and sales data.
    """
    try:
        vendor_sales_summary = pd.read_sql_query("""
            WITH FreightSummary AS (
                SELECT
                    VendorNumber,
                    SUM(Freight) AS Freight_Cost
                FROM vendor_invoice
                GROUP BY VendorNumber
            ),

            PurchaseSummary AS (
                SELECT
                    p.VendorNumber,
                    p.VendorName,
                    p.Brand,
                    p.Description,
                    p.PurchasePrice,
                    pp.Price AS ActualPrice,
                    pp.Volume,
                    SUM(p.Quantity) AS TotalPurchaseQuantity,
                    SUM(p.Dollars) AS TotalPurchaseDollars
                FROM purchases p
                JOIN purchase_prices pp ON p.Brand = pp.Brand
                WHERE p.PurchasePrice > 0
                GROUP BY 
                    p.VendorNumber, p.VendorName, p.Brand, 
                    p.Description, p.PurchasePrice, pp.Price, pp.Volume
            ),

            SalesSummary AS (
                SELECT
                    VendorNo,
                    Brand,
                    SUM(SalesQuantity) AS TotalSalesQuantity,
                    SUM(SalesDollars) AS TotalSalesDollars,
                    SUM(SalesPrice) AS TotalSalesPrice,
                    SUM(ExciseTax) AS TotalExciseTax
                FROM sales
                GROUP BY VendorNo, Brand
            )

            SELECT
                ps.VendorNumber,
                ps.VendorName,
                ps.Brand,
                ps.Description,
                ps.PurchasePrice,
                ps.ActualPrice,
                ps.Volume,
                ps.TotalPurchaseQuantity,
                ps.TotalPurchaseDollars,
                ss.TotalSalesQuantity,
                ss.TotalSalesDollars,
                ss.TotalSalesPrice,
                ss.TotalExciseTax,
                fs.Freight_Cost
            FROM PurchaseSummary ps
            LEFT JOIN SalesSummary ss
                ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
            LEFT JOIN FreightSummary fs
                ON ps.VendorNumber = fs.VendorNumber
            ORDER BY ps.TotalPurchaseDollars DESC
        """, conn)

        logging.info("Vendor summary generated successfully.")
        return vendor_sales_summary

    except Exception as e:
        logging.error(f"Failed to generate vendor summary: {e}")
        return None

def clean_data(df):
    """
    This function cleans the vendor sales summary DataFrame.
    """
    try:
        # Changing datatype to float
        df['Volume'] = df['Volume'].astype(float)

        # Filling missing values with 0
        df.fillna(0, inplace=True)

        # Removing spaces from categorical columns
        df['VendorName'] = df['VendorName'].str.strip()
        df['Description'] = df['Description'].str.strip()

        # Creating new columns for analysis
        df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
        df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
        df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
        df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

        logging.info("Data cleaning completed.")
        return df

    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        return df

# Entry point
if __name__ == '__main__':
    # Creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)

    if summary_df is not None:
        logging.info(summary_df.head())

        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head())

        logging.info('Ingesting data...')
        ingest_db(clean_df, 'vendor_sales_summary', conn)

        logging.info('Completed')
    else:
        logging.error("Vendor summary is None. Process aborted.")
