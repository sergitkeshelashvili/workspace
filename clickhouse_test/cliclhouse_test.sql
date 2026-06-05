-- 1. Sales & Revenue Report
-- პროდუქტის სრულ იდენტიფიკაციას: შტრიხკოდი, დასახელება, შიდა კოდი და კატეგორიები (ძირითადი ჯგუფი და ქვეკატეგორია)
-- მომწოდებლი: რომელ მომწოდებელთან ასოცირდება ესა თუ ის გაყიდული საქონელი.
-- რა ფასში იყიდებოდა პროდუქტი. სულ რა რაოდენობა გაიყიდა (წმინდა გაყიდვები: გაყიდვებს მინუს დაბრუნებები).
-- ჯამური შემოსავალი (Revenue): რა თანხა შემოვიდა ამ პროდუქტიდან.

WITH FilteredReceipts AS (
    -- ვფილტრავთ მონაცემებს რომ ram ლიმიტს არ აცდეს
    SELECT r.receipt_id, r.batch_id, r.optype, r.rec_status, br.pos_num
    FROM admin_Lake.APEX_axPMARKET_apos_POS_Receipts AS r
    INNER JOIN admin_Lake.APEX_axPMARKET_apos_POS_Batches AS br ON br.batch_id = r.batch_id
    WHERE br.opdate BETWEEN '2026-01-01' AND '2026-06-05'
      AND r.rec_status = 4
),

SalesData AS (
    SELECT
        pp.prodpp_id AS product_id,
        pr.BCode AS barcode,
        pro.Products_nu AS product_name,
        pp.scount AS quantity_sold,
        pp.priceg AS retail_price,
        pr.InCode AS internal_code,
        pp.vg AS sales_revenue,
        cat.Category_nu AS main_category,
        pc.PPCat_Nu AS sub_category,
        concat(l.cr, ' - ', a.Acc_nu) AS supplier_info
    FROM admin_Lake.APEX_axPMARKET_apos_POS_ReceiptProds AS pp
    INNER JOIN FilteredReceipts AS r ON pp.receipt_id = r.receipt_id

    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_ProdPP AS pr ON pp.prodpp_id = pr.ProdPP_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Products AS pro ON pro.Products_id = pr.Products_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Category AS cat ON cat.Category_id = pro.Category_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_PPCat AS pc ON pc.PPCat_ID = pr.PPCat_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_tmpt_LastSuppData AS l ON l.prodpp_id = pp.prodpp_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Accounts AS a ON a.Acc = l.cr
    WHERE pp.status = 4
      AND (
          (pp.rec_type = 1 AND r.optype = 1)
          OR (pp.rec_type = 5 AND r.optype = 2)
      )
)

-- 2. საბოლოო აგრეგაცია შესაბამისი ალიასებით
SELECT
    ifNull(barcode, toString(product_id)) AS Barcode,
    product_name                          AS ProductName,
    internal_code                         AS InternalCode,
    main_category                         AS MainCategory,
    sub_category                          AS SubCategory,
    retail_price                          AS RetailPrice,
    supplier_info                         AS SupplierInfo,
    sum(quantity_sold)                    AS TotalQuantitySold,
    sum(sales_revenue)                    AS TotalRevenue
FROM SalesData
GROUP BY
    ifNull(barcode, toString(product_id)),
    product_name,
    internal_code,
    main_category,
    sub_category,
    retail_price,
    supplier_info
ORDER BY TotalRevenue DESC

SETTINGS
    join_use_nulls = 1,
    -- თუ მეხსიერება არ ეყო, ClickHouse დააჯგუფებს დისკზე
    max_bytes_before_external_group_by = 20000000000,
    -- ჯოინების ოპტიმიზაციისთვის დისკის გამოყენება
    max_bytes_before_external_sort = 10000000000;
