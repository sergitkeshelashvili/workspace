-- gold_fact_sales_transactions
-- მომწოდებლის ბოლო ფასი და თვითღირებულება
WITH
    ProductCost AS (
        SELECT
            toString(ProdPPID) AS dls_prod_key,
            argMax(ValueNet, `Posting Date`) AS unit_cost_net,
            argMax(CreditAccountKey, `Posting Date`) AS supplier_account_key
        FROM admin_WH.Dim_Last_Supplier
        GROUP BY dls_prod_key
    )
SELECT
    toDate(rp.`Snapshot Day`) AS snapshot_day,
    toDateTime(rp.`Snapshot Day`) AS operation_datetime,
    toHour(toDateTime(rp.`Snapshot Day`)) AS operation_hour,
    rp.ReceiptID AS receipt_id,
    rp.`Dim_Products Key` AS product_key,
    ifNull(pc.supplier_account_key, 'N/A') AS supplier_account_key,
    rp.ReceiptType AS receipt_type,

    CASE WHEN rp.ReceiptType = 5 THEN -1 * abs(rp.SaleCount) ELSE rp.SaleCount END AS quantity_sold,
    CASE WHEN rp.ReceiptType = 5 THEN -1 * abs(rp.ValueGross) ELSE rp.ValueGross END AS gross_revenue,

    round(quantity_sold * ifNull(pc.unit_cost_net, 0), 2) AS total_cost_value,
    round(gross_revenue - total_cost_value, 2) AS net_profit_margin
FROM admin_WH.Fact_Receipt_Products AS rp
INNER JOIN admin_WH.Fact_Receipt AS r
    ON toString(rp.ReceiptID) = r.`Source ID`
LEFT JOIN ProductCost AS pc
    ON toString(rp.`Dim_Products Key`) = pc.dls_prod_key
WHERE rp.ReceiptStatus = 4
  AND r.ReceiptStatus = 4
  AND ((rp.ReceiptType = 1 AND r.OperationType = 1) OR (rp.ReceiptType = 5 AND r.OperationType = 2));



-- gold_dim_products
SELECT
    p.`Dim_Products Key` AS product_key,
    p.Barcode AS barcode,
    ifNull(p.ProductName, 'Unknown Product') AS product_name,
    ifNull(p.InternalCode, '') AS internal_code,
    p.UnitName AS unit_name,
    p.IsSoldByWeight AS is_weight_item,

    ifNull(c1.Category, 'სხვა L1') AS category_level_1,
    ifNull(c2.Category, 'სხვა L2') AS category_level_2,
    ifNull(c3.Category, 'სხვა L3') AS category_level_3
FROM admin_WH.Dim_Products AS p
LEFT JOIN admin_WH.Dict_Category_L1 AS c1 ON CAST(p.`Dict_Category_L1 Key` AS UInt128) = c1.`Dict_Category_L1 Key`
LEFT JOIN admin_WH.Dict_Category_L2 AS c2 ON CAST(p.`Dict_Category_L2 Key` AS UInt128) = c2.`Dict_Category_L2 Key`
LEFT JOIN admin_WH.Dict_Category_L3 AS c3 ON CAST(p.`Dict_Category_L3 Key` AS UInt128) = c3.`Dict_Category_L3 Key`
WHERE p.`Delete Flag` = 0;





-- (Semantic Layer ან Prompt Context): ბუღალტრული წიგნის debit_account_key და credit_account_key (Int64) 
-- ემთხვევა მომწოდებლების ცხრილის (gold_dim_suppliers_and_accounts) სვეტს: 
-- supplier_id (Source ID / CustomerID - რომელიც არის რეალური ანგარიშის კოდი ბაზაში)."

--  gold_dim_suppliers_and_accounts

SELECT
    `Dim_Accounts Key` AS account_key,
    `Source ID` AS supplier_id,
    ifNull(AccountName, 'Unknown Account') AS supplier_name,
    ifNull(TaxID_PersonalNumber, 'N/A') AS tax_id,
    ifNull(LegalAddress, 'N/A') AS legal_address,
    CardType AS card_type,
    Currency AS account_currency
FROM admin_WH.Dim_Accounts
WHERE `Delete Flag` = 0;



-- gold_daily_financial_stats ყოველდღიური ფინანსური ბრუნვები

SELECT
    toDate(`Snapshot Day`) AS snapshot_day,
    DebitAccountKey AS debit_account_key,
    CreditAccountKey AS credit_account_key,
    CurrencyID AS currency_id,

    -- ფინანსური მოცულობები
    round(sum(ifNull(ValueGross, 0)), 2) AS total_gross_amount,
    round(sum(ifNull(ValueNet, 0)), 2) AS total_net_amount,
    round(sum(ifNull(ValueCost, 0)), 2) AS total_cost_amount,

    count() AS total_transactions_count
FROM admin_WH.Fact_Book
GROUP BY
    snapshot_day,
    debit_account_key,
    credit_account_key,
    currency_id;
