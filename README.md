# 📘 Scenario: Retail Product Catalog UPSERTS

A retail company receives **daily updates** for its product catalog, including:  
- 🆕 New products  
- 💰 Price changes  
- ❌ Discontinued items  

Instead of overwriting the entire catalog or simply appending new records, they need to **upsert** the incoming data:  

- 🔄 Updating existing products with the **latest information**  
- ➕ Inserting new products when they appear  

This ensures the product catalog remains **accurate and up-to-date in real-time**.
