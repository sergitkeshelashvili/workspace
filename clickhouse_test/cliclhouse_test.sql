-- 1. Sales & Revenue Report
-- პროდუქტის სრულ იდენტიფიკაციას: შტრიხკოდი, დასახელება, შიდა კოდი და კატეგორიები (ძირითადი ჯგუფი და ქვეკატეგორია)
-- მომწოდებლი: რომელ მომწოდებელთან ასოცირდება ესა თუ ის გაყიდული საქონელი.
-- რა ფასში იყიდებოდა პროდუქტი. სულ რა რაოდენობა გაიყიდა (წმინდა გაყიდვები: გაყიდვებს მინუს დაბრუნებები).
-- ჯამური შემოსავალი (Revenue): რა თანხა შემოვიდა ამ პროდუქტიდან.

WITH SalesData AS (
    SELECT
        pp.prodpp_id AS product_id,
        pr.BCode AS barcode,
        pro.Products_nu AS product_name,
        pr.InCode AS internal_code,
        cat.Category_nu AS main_category,
        pc.PPCat_Nu AS sub_category,
        concat(l.cr, ' - ', a.Acc_nu) AS supplier_info,
    
        CASE
            WHEN pp.rec_type = 1 THEN pp.scount
            WHEN pp.rec_type = 5 THEN -1 * abs(pp.scount)
            ELSE 0
        END AS net_quantity,

        CASE
            WHEN pp.rec_type = 1 THEN pp.vg
            WHEN pp.rec_type = 5 THEN -1 * abs(pp.vg)
            ELSE 0
        END AS net_revenue

    FROM admin_Lake.APEX_axPMARKET_apos_POS_ReceiptProds AS pp
    INNER JOIN admin_Lake.APEX_axPMARKET_apos_POS_Receipts AS r ON pp.receipt_id = r.receipt_id
    INNER JOIN admin_Lake.APEX_axPMARKET_apos_POS_Batches AS br ON br.batch_id = r.batch_id
    INNER JOIN admin_Lake.APEX_axPMARKET_apos_POS_BookProp AS bp ON bp.pos_id = br.pos_num
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Branchs AS bch ON bch.br = bp.branch_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_ProdPP AS pr ON pp.prodpp_id = pr.ProdPP_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Products AS pro ON pro.Products_id = pr.Products_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Category AS cat ON cat.Category_id = pro.Category_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_PPCat AS pc ON pc.PPCat_ID = pr.PPCat_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_tmpt_LastSuppData AS l ON l.prodpp_id = pp.prodpp_id
    LEFT JOIN admin_Lake.APEX_axPMARKET_dbo_Accounts AS a ON a.Acc = l.cr
    WHERE pp.status = 4
      AND r.rec_status = 4
      AND (
          (pp.rec_type = 1 AND r.optype = 1)
          OR (pp.rec_type = 5 AND r.optype = 2)
      )
      -- თარიღების ფილტრი
      AND br.opdate BETWEEN '2026-01-01' AND '2026-01-02'
)

-- 3. საბოლოო რეპორტი
SELECT
    ifNull(barcode, toString(product_id)) AS Barcode,
    product_name                          AS ProductName,
    internal_code                         AS InternalCode,
    main_category                         AS MainCategory,
    sub_category                          AS SubCategory,
    supplier_info                         AS SupplierInfo,

    -- გამოთვლილი საშუალო გასაყიდი ფასი (რომ პროდუქტი არ გაორდეს ფასის ცვლილებისას)
    round(sum(net_revenue) / nullIf(sum(net_quantity), 0), 2) AS AverageRetailPrice,

    -- ბიზნეს მეტრიკები (KPIs)
    sum(net_quantity)                    AS TotalQuantitySold,
    sum(net_revenue)                     AS TotalRevenue
FROM SalesData
GROUP BY
    ifNull(barcode, toString(product_id)),
    product_name,
    internal_code,
    main_category,
    sub_category,
    supplier_info
ORDER BY TotalRevenue DESC
SETTINGS
    join_use_nulls = 1,
    max_bytes_before_external_group_by = 20000000000,
    max_bytes_before_external_sort = 10000000000;

----------------------------------------------------------------
---------------------------------------------------------------

---- query 2.

WITH
    '2026-01-01'::Date AS param_d1,
    '2026-01-02'::Date AS param_d2,
    NULL         AS param_db,
    NULL         AS param_cr,
    NULL         AS param_BCode,
    NULL         AS param_Category_id,
    NULL         AS param_ppcat,
    NULL         AS param_pcat,
    NULL         AS param_client,

-- 1. ფილტრული Book-ები
FilteredBook AS (
    SELECT Book_id, dDate, dtype, cr, db, Docs_id, NumberIn, NumberOut
    FROM admin_Lake.APEX_axPMARKET_dbo_Book
    WHERE toDate(dDate) BETWEEN param_d1 AND param_d2
      AND dtype IN (2, 3)
      AND (param_db IS NULL OR cr = param_db)
),

-- 2.

FilteredB3 AS (
    SELECT b3.Book_id, b3.db, b3.cr, b3.Docs_id, b3.OpDet_id
    FROM admin_Lake.APEX_axPMARKET_dbo_Book AS b3
    INNER JOIN FilteredBook AS fb ON b3.Docs_id = fb.Docs_id
    WHERE b3.OpDet_id LIKE '%02'
      AND (param_client IS NULL OR b3.db = param_client)
),

-- 3. Orders-ის აგრეგაცია ჯოინების გარეშე
AggregatedOrders AS (
    SELECT
        Book_Id,
        Supplies_Id,
        Vat,
        sum(Vg)     AS total_vg,
        sum(SCount) AS total_scount
    FROM admin_Lake.APEX_axPMARKET_dbo_Orders
    WHERE Book_Id IN (SELECT Book_id FROM FilteredBook)
    GROUP BY Book_Id, Supplies_Id, Vat
),

-- 4. Supplies pre-filter
FilteredSupplies AS (
    SELECT
        s.Supplies_id,
        s.ProdPP_id,
        s.Vg,
        s.VcustomG,
        s.ICount,
        s.Cr_id
    FROM admin_Lake.`APEX_axPMARKET_dbo_Supplies (custom)` AS s
    WHERE s.Supplies_id IN (SELECT Supplies_Id FROM AggregatedOrders)
),

-- 5.
FilteredB2 AS (
    SELECT DISTINCT b2.Book_id, b2.cr, b2.NumberIn, b2.NumberOut, b2.dDate
    FROM admin_Lake.APEX_axPMARKET_dbo_Book AS b2
    WHERE b2.Book_id IN (SELECT Cr_id FROM FilteredSupplies)
),

-- 6. ძირითადი გაანგარიშება
sales AS (
    SELECT
        s.ProdPP_id                                                                       AS prodpp_id,
        b.dDate                                                                           AS ddate,
        o.Vat                                                                             AS vat,
        o.Book_Id                                                                         AS book_id,
        b2.Book_id                                                                        AS sbook_id,
        max(b2.cr)                                                                        AS cr,
        sum(o.total_vg)                                                                   AS vg,
        sum(o.total_vg / (1 + o.Vat))                                                    AS vgd,
        sum(o.total_scount * (CAST(s.Vg AS Float64) + CAST(s.VcustomG AS Float64))
            / if(CAST(s.ICount AS Float64) = 0, 1, CAST(s.ICount AS Float64)))           AS cost,
        sum(o.total_vg / (1 + o.Vat))
            - sum(o.total_scount * (CAST(s.Vg AS Float64) + CAST(s.VcustomG AS Float64))
              / if(CAST(s.ICount AS Float64) = 0, 1, CAST(s.ICount AS Float64)))         AS margin,
        sum(o.total_scount)                                                               AS scount
    FROM AggregatedOrders AS o
    INNER JOIN FilteredSupplies AS s  ON o.Supplies_Id = s.Supplies_id
    INNER JOIN FilteredBook     AS b  ON o.Book_Id     = b.Book_id
    INNER JOIN FilteredB2       AS b2 ON s.Cr_id       = b2.Book_id
    WHERE (param_cr IS NULL OR b2.cr = param_cr)
    GROUP BY
        o.Book_Id,
        b.dDate,
        s.ProdPP_id,
        o.Vat,
        b2.Book_id
),

-- 7. ProdView pre-filter
FilteredProdView AS (
    SELECT
        pp.ProdPP_id,
        pp.BCode,
        pp.Products_nu,
        pp.PPCat_Nu,
        pp.Category_nu,
        pp.Category_Code,
        pp.Category_id,
        pp.PPCat_id,
        pp.pcat_id,
        pp.Lown,
        pp.Producer_nu,
        pp.Vat       AS productvat,
        pp.VatType
    FROM admin_Lake.APEX_axPMARKET_dbo_ProdView AS pp
    WHERE pp.ProdPP_id IN (SELECT prodpp_id FROM sales)
      AND (param_BCode       IS NULL OR pp.BCode        = param_BCode)
      AND (param_Category_id IS NULL OR pp.Category_id  = param_Category_id)
      AND (param_ppcat       IS NULL OR pp.PPCat_id      = param_ppcat)
),


cc AS (
    SELECT
        s.vg, s.vgd, s.cost, s.margin, s.scount,
        a3.AccAlt                        AS accalt,
        concat(b3.db, ' - ', a3.Acc_nu) AS db2,
        b3.cr                            AS cr2,
        b.db                             AS db,
        b.cr                             AS cr,
        a.Acc_nu                         AS Acc_nu,
        s.vat                            AS Vat,
        pp.BCode                         AS bcode,
        s.ddate                          AS ddate,
        pp.Products_nu                   AS productsn,
        pp.PPCat_Nu                      AS PPCat_Nu,
        s.cr                             AS crm,
        d.Oper_id                        AS opdetid,
        pp.Category_nu                   AS category_nu,
        pp.Category_Code                 AS categorycode,
        pp.Lown                          AS lown,
        pp.Producer_nu                   AS Producer_nu,
        pp.productvat                    AS productvat,
        b.NumberIn                       AS numberin,
        b.NumberOut                      AS numberout,
        d.PresalerID                     AS presalerid,
        b2.NumberIn                      AS numberinIn,
        b2.NumberOut                     AS numberoutIn,
        b2.dDate                         AS ddateIn,
        toYear(b.dDate)                  AS nyear,
        toMonth(b.dDate)                 AS nmonth,
        toDayOfMonth(b.dDate)            AS nday,
        toDayOfWeek(b.dDate)             AS nweekday,
        s.book_id                        AS book_id,
        pp.VatType                       AS vattype,
        ap.hname                         AS pcat_nu
    FROM sales AS s
    INNER JOIN FilteredBook              AS b  ON s.book_id   = b.Book_id
    INNER JOIN FilteredProdView          AS pp ON s.prodpp_id = pp.ProdPP_id
    INNER JOIN FilteredB2               AS b2 ON s.sbook_id  = b2.Book_id
    INNER JOIN FilteredB3               AS b3 ON b.Docs_id   = b3.Docs_id
    INNER JOIN admin_Lake.APEX_axPMARKET_dbo_Docs     AS d  ON b.Docs_id = d.Docs_id
    INNER JOIN admin_Lake.APEX_axPMARKET_dbo_Accounts AS a3 ON b3.db     = a3.Acc
    INNER JOIN admin_Lake.APEX_axPMARKET_dbo_Accounts AS a  ON a.Acc     = b.cr
    LEFT  JOIN admin_Lake.APEX_axPMARKET_aap_pCat     AS ap ON ap.codeid = pp.pcat_id
    WHERE (param_pcat IS NULL OR ap.codeid = param_pcat)
)

-- 9. საბოლოო SELECT 
SELECT
    aa.*,
    c.sn AS sn
FROM cc AS aa
INNER JOIN admin_Lake.APEX_axPMARKET_acc_AccountDetails AS c ON c.acc = aa.crm

SETTINGS
    join_use_nulls                    = 1,
    join_algorithm                    = 'grace_hash',
    grace_hash_join_initial_buckets   = 16,
    grace_hash_join_max_buckets       = 256,
    max_bytes_in_join                 = 2000000000,
    max_bytes_before_external_group_by = 2000000000,
    max_bytes_before_external_sort    = 2000000000;








