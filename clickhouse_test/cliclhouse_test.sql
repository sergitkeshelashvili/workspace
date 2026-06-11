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
      
      AND br.opdate BETWEEN '2026-01-01' AND '2026-01-02'
      AND (0 = 0 OR br.pos_num = 0)
      AND ('' = '' OR pro.Category_id = '')
      AND ('' = '' OR pr.PPCat_id = '')
      AND ('' = '' OR bp.branch_id = '')
)

SELECT
    ifNull(barcode, toString(product_id)) AS Barcode,
    product_name                          AS ProductName,
    internal_code                         AS InternalCode,
    main_category                         AS MainCategory,
    sub_category                          AS SubCategory,
    supplier_info                         AS SupplierInfo,

    round(sum(net_revenue) / nullIf(sum(net_quantity), 0), 2) AS AverageRetailPrice,

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




---------------------- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Parametrized views



-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║  B2B / საბითუმო გაყიდვები — პროდუქტებით, მარჟა, თვითღირ.              ║
-- ║  წყარო: Book → Orders → Supplies (ბუღალტრული pipeline)                 ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  📌 პარამეტრები (Unistream UI-დან):                                     ║
-- ║     {start_date:Date32} — პერიოდის დასაწყისი                           ║
-- ║     {end_date:Date32}   — პერიოდის დასასრული                           ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  📊 რა ვიზომავთ:                                                        ║
-- ║    • შემოსავალი (VAT-ით / VAT-გარეშე)                                   ║
-- ║    • თვითღირებულება = სcount × (Vg + VcustomG) / ICount                ║
-- ║    • მარჟა (₾) = შემ. (ვდგ.) − თვითღირ.                               ║
-- ║    • კლიენტი (b3.db, OpDet '%02') + მომწოდებელი (b2.cr)               ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  ⚙️  dtype  2 = შეძენა,  3 = გაყიდვა             (Book.dtype)          ║
-- ║      OpDet '%02' = საქონლის ღირებულების გატარება  (Book.OpDet_id)       ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  🔧 ოპტიმიზაცია vs ორიგინალი:                                          ║
-- ║    ✅ Book სკანდება მხოლოდ 1-ჯერ, date+dtype ფილტრით (FilteredBook)    ║
-- ║    ✅ Orders/Supplies/b3 — IN(subquery) pre-filtering, full scan-ი არ   ║
-- ║    ✅ ProdView pre-filtered მხოლოდ საჭირო ProdPP_id-ებზე               ║
-- ║    ✅ grace_hash_join — large JOIN-ებისთვის disk spillover               ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

WITH

-- ─── CTE 1: FilteredBook ───────────────────────────────────────────────────
-- 🎯 PIPELINE-ის „კარი" — ამ CTE-ს გარეშე ყველა სხვა JOIN სრულ ცხრილებს
--    სკანავდა. გაფილტვრა date + dtype-ით მნიშვნელოვნად ამცირებს მოცულობას.
--    dtype=2 = შეძენა | dtype=3 = გაყიდვა
-- ──────────────────────────────────────────────────────────────────────────
FilteredBook AS (
    SELECT
        b."Book_id"  AS Book_id,
        b."Docs_id"  AS Docs_id,   -- Docs.Docs_id—ის reference (b3 join-ისთვის)
        b."dDate"    AS dDate,
        b."dtype"    AS dtype,
        b."cr"       AS cr,
        b."db"       AS db
    FROM admin_Lake."APEX_axPMARKET_dbo_Book" AS b
    WHERE toDate(b."dDate") BETWEEN {start_date:Date32} AND {end_date:Date32}
      AND b."dtype" IN (2, 3)
),

-- ─── CTE 2: FilteredOrders ────────────────────────────────────────────────
-- 🎯 Orders — მხოლოდ FilteredBook-ში შემავალი journal entry-ები.
--    PRE-AGGREGATION აქვე: ერთი Supplies_Id-ისთვის ერთი row → Supplies-თან
--    join-ი გამარტივდება და შემცირდება მეხსიერების მოხმარება.
-- ──────────────────────────────────────────────────────────────────────────
FilteredOrders AS (
    SELECT
        o."Book_Id"     AS Book_Id,
        o."Supplies_Id" AS Supplies_Id,
        o."Vat"         AS Vat,
        sum(o."Vg")     AS total_vg,
        sum(o."SCount") AS total_scount
    FROM admin_Lake."APEX_axPMARKET_dbo_Orders" AS o
    WHERE o."Book_Id" IN (SELECT Book_id FROM FilteredBook)
    GROUP BY
        o."Book_Id",
        o."Supplies_Id",
        o."Vat"
),

-- ─── CTE 3: FilteredSupplies ──────────────────────────────────────────────
-- 🎯 Supplies — მხოლოდ FilteredOrders-ში referenced Supplies.
--    Cr_id → b2-ს Book_id (მომწოდებლის ანგარიშ-ფაქტურა).
--    Vg, VcustomG, ICount → თვითღირებულების ფორმულისთვის.
--    ⚠️  Supplies ცხრილში ყველა колона String-ია, ამიტომ CAST საჭიროა.
-- ──────────────────────────────────────────────────────────────────────────
FilteredSupplies AS (
    SELECT
        s."Supplies_id"        AS Supplies_id,
        s."ProdPP_id"          AS ProdPP_id,
        s."Vg"                 AS Vg,
        s."VcustomG"           AS VcustomG,
        s."ICount"             AS ICount,
        s."Cr_id"              AS Cr_id
    FROM admin_Lake."APEX_axPMARKET_dbo_Supplies" AS s
    WHERE s."Supplies_id" IN (SELECT Supplies_Id FROM FilteredOrders)
),

-- ─── CTE 4: SupplySourceBook (b2) ─────────────────────────────────────────
-- 🎯 b2 = მომწოდებლის Book ჩანაწერი (შეძენის ანგარიშ-ფაქტურა).
--    Supplies.Cr_id → b2.Book_id → b2.cr = მომწოდებლის Accounts.Acc
--    DISTINCT: ერთი Book_id → ერთი cr (max-ის გამოყენება CoreData-ში)
-- ──────────────────────────────────────────────────────────────────────────
SupplySourceBook AS (
    SELECT DISTINCT
        b2."Book_id" AS Book_id,
        b2."cr"      AS cr
    FROM admin_Lake."APEX_axPMARKET_dbo_Book" AS b2
    WHERE b2."Book_id" IN (SELECT Cr_id FROM FilteredSupplies)
),

-- ─── CTE 5: ClientBook (b3) ───────────────────────────────────────────────
-- 🎯 b3 = კლიენტის ანგარიშის ჩანაწერი.
--    b3.Docs_id = FilteredBook.Docs_id  ← სწორი join!
--    ერთ Docs დოკუმენტს (Docs_id) რამდენიმე Book ჩანაწერი ეკუთვნის.
--    ჩვენ გვაინტერესებს ის ჩანაწერი, სადაც OpDet LIKE '%02'
--    (საქონლის ღირებულება), რის db ველიც = კლიენტის Accounts.Acc.
-- ──────────────────────────────────────────────────────────────────────────
ClientBook AS (
    SELECT
        b3."Docs_id"  AS Docs_id,   -- = FilteredBook.Docs_id
        b3."db"       AS db,        -- კლიენტის ანგარიში
        b3."OpDet_id" AS OpDet_id
    FROM admin_Lake."APEX_axPMARKET_dbo_Book" AS b3
    WHERE b3."Docs_id" IN (SELECT Docs_id FROM FilteredBook)
      AND b3."OpDet_id" LIKE '%02'
),

-- ─── CTE 6: CoreData ──────────────────────────────────────────────────────
-- 🎯 ძირითადი ფინანსური გაანგარიშება.
--
--    vg   = გაყ. ფასი VAT-ით (ჯამი)
--    vgd  = გაყ. ფასი VAT-გარეშე  = vg / (1 + Vat)
--    cost = თვითღირ. = SCount × (Vg + VcustomG) / ICount
--             ICount=0 → განყოფა 0-ზე თავიდან ასაცილებლად: if(...,1,...)
--    margin = vgd − cost  (მარჟა ₾-ში, ვდგ. გარეშე)
--
--    ⚠️  margin-ი alias-ის ხელახლა გამოყენებით NE იხმარება —
--        გამოსახულება სრულად იწერება (ClickHouse alias substitution-ის
--        პოტენციური side-effect-ის თავიდან ასაცილებლად)
-- ──────────────────────────────────────────────────────────────────────────
CoreData AS (
    SELECT
        s."ProdPP_id"                                        AS ProdPP_id,
        b."dDate"                                            AS dDate,
        o."Vat"                                              AS Vat,
        o."Book_Id"                                          AS book_id,
        b."Docs_id"                                          AS docs_id,  -- ClientBook join-ისთვის
        b2."Book_id"                                         AS sbook_id,
        max(b2."cr")                                         AS cr,

        -- შემოსავლები
        sum(o."total_vg")                                    AS vg,
        sum(o."total_vg" / (1 + o."Vat"))                   AS vgd,

        -- თვითღირებულება
        sum(
            o."total_scount"
            * (CAST(s."Vg"       AS Float64)
             + CAST(s."VcustomG" AS Float64))
            / if(CAST(s."ICount" AS Float64) = 0,
                 1,
                 CAST(s."ICount" AS Float64))
        )                                                    AS cost,

        -- მარჟა = vgd − cost  (გამოსახულება სრულად, alias-გარეშე)
        sum(o."total_vg" / (1 + o."Vat"))
        - sum(
            o."total_scount"
            * (CAST(s."Vg"       AS Float64)
             + CAST(s."VcustomG" AS Float64))
            / if(CAST(s."ICount" AS Float64) = 0,
                 1,
                 CAST(s."ICount" AS Float64))
          )                                                  AS margin,

        sum(o."total_scount")                                AS scount

    FROM FilteredOrders   AS o
    INNER JOIN FilteredSupplies  AS s  ON o."Supplies_Id" = s."Supplies_id"
    INNER JOIN FilteredBook      AS b  ON o."Book_Id"     = b."Book_id"
    INNER JOIN SupplySourceBook  AS b2 ON s."Cr_id"       = b2."Book_id"
    GROUP BY
        o."Book_Id",
        b."Docs_id",
        b."dDate",
        s."ProdPP_id",
        o."Vat",
        b2."Book_id"
),

-- ─── CTE 7: FilteredProdView ──────────────────────────────────────────────
-- 🎯 პროდუქტის ბარათი — მხოლოდ CoreData-ში referenced ProdPP_id-ები.
--    სრული ProdView სკანის თავიდან ასაცილებლად.
-- ──────────────────────────────────────────────────────────────────────────
FilteredProdView AS (
    SELECT
        pp."ProdPP_id"   AS ProdPP_id,
        pp."BCode"       AS BCode,
        pp."Products_nu" AS Products_nu,
        pp."Category_nu" AS Category_nu
    FROM admin_Lake."APEX_axPMARKET_dbo_ProdView" AS pp
    WHERE pp."ProdPP_id" IN (SELECT ProdPP_id FROM CoreData)
)

-- ─── Final SELECT ──────────────────────────────────────────────────────────
-- ყველა CTE-ს შეერთება.
--   Accounts a3 → კლიენტის სახელი    (ClientBook.db)
--   Accounts a  → მომწოდებლის სახელი  (CoreData.cr = SupplySourceBook.cr)
--   AccountDetails accd → მომწოდებლის პ/ნ
-- ──────────────────────────────────────────────────────────────────────────
SELECT
    cd."ProdPP_id"                      AS "Product_Internal_ID",
    toDate(cd."dDate")                  AS "Operation_Date",
    cd."Vat"                            AS "VAT_Rate",
    cd."book_id"                        AS "Journal_ID",
    pp."BCode"                          AS "Barcode",
    pp."Products_nu"                    AS "Product_Name",
    pp."Category_nu"                    AS "Main_Category",
    round(cd."vg",      2)              AS "Total_Revenue_Incl_VAT",
    round(cd."vgd",     2)              AS "Revenue_Excl_VAT",
    round(cd."cost",    2)              AS "Product_Cost_Price",
    round(cd."margin",  2)              AS "Profit_Margin",
    round(cd."scount",  3)              AS "Quantity_Sold",
    concat(cb."db", ' - ', a3."Acc_nu") AS "Client_Info",
    a."Acc_nu"                          AS "Supplier_Name",
    accd."sn"                           AS "Account_Details"

FROM CoreData AS cd
INNER JOIN FilteredProdView                                   AS pp
    ON pp."ProdPP_id" = cd."ProdPP_id"
INNER JOIN ClientBook                                         AS cb
    ON cb."Docs_id"   = cd."docs_id"
INNER JOIN admin_Lake."APEX_axPMARKET_dbo_Accounts"           AS a3
    ON a3."Acc"       = cb."db"
INNER JOIN admin_Lake."APEX_axPMARKET_dbo_Accounts"           AS a
    ON a."Acc"        = cd."cr"
LEFT  JOIN admin_Lake."APEX_axPMARKET_acc_AccountDetails"     AS accd
    ON accd."acc"     = cd."cr"

ORDER BY
    toDate(cd."dDate") DESC,
    cd."margin"        DESC




------ mereo


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║  POS გაყიდვები — დეტალური პროდუქტებით (გაყიდვა + დაბრუნება)           ║
-- ║  წყარო: სალიერო ქსელი (POS) — ReceiptProds → Receipts → Batches        ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  📌 პარამეტრები (Unistream UI-დან):                                     ║
-- ║     {start_date:Date32}     — პერიოდის დასაწყისი (ჩათვლით)             ║
-- ║     {end_date:Date32}       — პერიოდის დასასრული  (ჩათვლით)            ║
-- ║     {filter_by_pos:Float64} — 0 = ყველა POS-ი, 1 = კონკრეტული POS     ║
-- ║     {pos_number:Float64}    — POS ნომერი ({filter_by_pos}=1-ის შემთხვ.) ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  📊 გამოსავალი ველები:                                                  ║
-- ║    Barcode               — ბარკოდი (ან Product_ID თუ ბარკოდი ცარიელია) ║
-- ║    Product_Name          — პროდუქტის სახელი                             ║
-- ║    Internal_Code         — შიდა კოდი (InCode)                           ║
-- ║    Main_Category         — მთავარი კატეგორია (Category)                 ║
-- ║    Sub_Category          — ქვეკატეგორია (PPCat)                         ║
-- ║    Supplier_Info         — მომწოდებლის Acc + სახელი                     ║
-- ║    Average_Retail_Price  — საშ. სარეალიზაციო ფასი (Net_Revenue/Qty)    ║
-- ║    Total_Quantity_Sold   — წმინდა გაყ. რაოდ. (გაყ. − დაბ.)             ║
-- ║    Total_Net_Revenue     — წმინდა შემოსავალი ₾ (გაყ. − დაბ.)           ║
-- ║─────────────────────────────────────────────────────────────────────────║
-- ║  ⚙️  rec_type = 1 → გაყიდვა    (optype = 1)  — დადებითი                ║
-- ║      rec_type = 5 → დაბრუნება  (optype = 2)  — უარყოფითი               ║
-- ║      status  = 4 → სამუშაო ჩანაწერი (ReceiptProds)                     ║
-- ║      rec_status = 4 → სამუშაო ჩეკი  (Receipts)                         ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

SELECT
    -- ბარკოდი: თუ ცარიელია → Product_ID hex სახით (ClickHouse ifNull)
    ifNull(Barcode, toString(Product_ID)) AS Barcode,

    Product_Name,
    Internal_Code,
    Main_Category,
    Sub_Category,
    Supplier_Info,

    -- საშ. სარეალიზაციო ფასი = ჯამური წმინდა შემოსავ. / ჯამური წმინდა რაოდ.
    -- nullIf(sum(Net_Quantity), 0) — 0-ზე განყოფის თავიდან ასაცილებლად
    round(sum(Net_Revenue) / nullIf(sum(Net_Quantity), 0), 2) AS Average_Retail_Price,

    sum(Net_Quantity) AS Total_Quantity_Sold,
    sum(Net_Revenue)  AS Total_Net_Revenue

FROM (
    -- ──────────────────────────────────────────────────────────────────────
    -- ქვე-SELECT: სტრიქ-დონის Net_Quantity / Net_Revenue გაანგარიშება
    --
    --   rec_type=1 (გაყიდვა)   → scount და vg უცვლელად (დადებითი)
    --   rec_type=5 (დაბრუნება) → scount და vg გამრავლება -1-ზე (უარყოფითი)
    --   სხვა rec_type           → 0 (ეს ჩანაწერები WHERE-ით გაიფილტრება)
    --
    --   ⚠️ abs() გამოიყენება rec_type=5-ზე, რადგან სისტემა ზოგჯერ
    --      დაბრუნების scount/vg-ს უარყოფითად ინახავს, ზოგჯერ — დადებითად.
    --      abs() → -1 = ყოველთვის უარყოფითი ნომინალი
    -- ──────────────────────────────────────────────────────────────────────
    SELECT
        -- პროდუქტის იდენტიფიკატორები
        pp."prodpp_id"    AS Product_ID,      -- ProdPP პირველადი გასაღები
        pr."BCode"        AS Barcode,         -- სავაჭრო ბარკოდი
        pro."Products_nu" AS Product_Name,    -- პროდუქტის სახელი (ქართ.)
        pr."InCode"       AS Internal_Code,   -- შიდა საწყობის კოდი

        -- კლასიფიკაცია
        cat."Category_nu" AS Main_Category,   -- მთავარი კატეგ. (Category_id → Category_nu)
        pc."PPCat_Nu"     AS Sub_Category,    -- ქვეკატეგ. (PPCat_id → PPCat_Nu)

        -- მომწოდებელი (ბოლო შეძენის ჩანაწერი tmpt_LastSuppData-დან)
        -- concat: "Acc_code - Acc_name" ფორმატი
        concat(l."cr", ' - ', a."Acc_nu") AS Supplier_Info,

        -- ──────────────────────────────────────────────────
        -- Net_Quantity: გაყიდვა (+) / დაბრუნება (-)
        -- ──────────────────────────────────────────────────
        CASE
            WHEN pp."rec_type" = 1 THEN pp."scount"               -- გაყიდვა → +scount
            WHEN pp."rec_type" = 5 THEN -1 * abs(pp."scount")     -- დაბრ.   → -|scount|
            ELSE 0
        END AS Net_Quantity,

        -- ──────────────────────────────────────────────────
        -- Net_Revenue: გაყიდვის / დაბრუნების ჯამური თანხა (VAT-ით)
        -- ──────────────────────────────────────────────────
        CASE
            WHEN pp."rec_type" = 1 THEN pp."vg"                   -- გაყიდვა → +vg
            WHEN pp."rec_type" = 5 THEN -1 * abs(pp."vg")         -- დაბრ.   → -|vg|
            ELSE 0
        END AS Net_Revenue

    -- ─── ძირითადი ცხრილი: POS ჩეკის სტრიქები ─────────────────────────────
    FROM admin_Lake."APEX_axPMARKET_apos_POS_ReceiptProds" AS pp

    -- ჩეკი (Receipts): rec_status = 4 → სამუშაო ჩეკი
    -- optype = 1 → გაყიდვა | optype = 2 → დაბრუნება
    INNER JOIN admin_Lake."APEX_axPMARKET_apos_POS_Receipts" AS r
        ON pp."receipt_id" = r."receipt_id"

    -- Batch (Batches): opdate ↔ {start_date}/{end_date} ფილტრი
    -- pos_num → POS ნომრის ფილტრი ({filter_by_pos} = 1-ის შემთხვ.)
    INNER JOIN admin_Lake."APEX_axPMARKET_apos_POS_Batches" AS br
        ON br."batch_id" = r."batch_id"

    -- ─── პროდუქტის ცნობარები (LEFT JOIN — შესაძლოა ზოგს არ ჰქონდეს) ─────
    LEFT JOIN admin_Lake."APEX_axPMARKET_dbo_ProdPP" AS pr
        ON pp."prodpp_id" = pr."ProdPP_id"   -- ProdPP: BCode, InCode, PPCat_id

    LEFT JOIN admin_Lake."APEX_axPMARKET_dbo_Products" AS pro
        ON pro."Products_id" = pr."Products_id"  -- Products: სახ., Category_id

    LEFT JOIN admin_Lake."APEX_axPMARKET_dbo_Category" AS cat
        ON cat."Category_id" = pro."Category_id"  -- Category: Category_nu

    LEFT JOIN admin_Lake."APEX_axPMARKET_dbo_PPCat" AS pc
        ON pc."PPCat_ID" = pr."PPCat_id"           -- PPCat: PPCat_Nu (ქვეკატ.)

    -- ─── მომწოდებელი: tmpt_LastSuppData → Accounts ────────────────────────
    -- LastSuppData: ბოლო შეძენის მომწოდებელი (cr = Accounts.Acc)
    LEFT JOIN admin_Lake."APEX_axPMARKET_tmpt_LastSuppData" AS l
        ON l."prodpp_id" = pp."prodpp_id"

    LEFT JOIN admin_Lake."APEX_axPMARKET_dbo_Accounts" AS a
        ON a."Acc" = l."cr"                        -- Accounts: Acc_nu სახელი

    -- ─── ფილტრები ──────────────────────────────────────────────────────────
    WHERE pp."status"    = 4              -- სამუშაო სტრიქი (4 = active)
      AND r."rec_status" = 4              -- სამუშაო ჩეკი  (4 = active)
      AND (
          (pp."rec_type" = 1 AND r."optype" = 1)   -- გაყიდვა
       OR (pp."rec_type" = 5 AND r."optype" = 2)   -- დაბრუნება
      )
      -- თარიღის ფილტრი: Batches.opdate (Date32) — POS-ის ოპერაციული თარიღი
      AND br."opdate" BETWEEN {start_date:Date32} AND {end_date:Date32}
      -- POS ფილტრი: 0 = ყველა, 1 = კონკრეტული pos_num
      AND ({filter_by_pos:Float64} = 0 OR br."pos_num" = {pos_number:Float64})
)

-- ─── აგრეგაცია პროდუქტ × კატეგ. × მომწოდ. ────────────────────────────────
-- ყველა ფილიალი / POS ჩეკი — ჯამდება ერთ სტრიქად ერთი პროდუქტ-კომბინაციაზე
GROUP BY
    Barcode,
    Product_Name,
    Internal_Code,
    Main_Category,
    Sub_Category,
    Supplier_Info

ORDER BY
    Total_Net_Revenue DESC






------------------------------------- @22222222222222222222222222222222  view დან ანალიტკა


SELECT
    Barcode,
    Product_Name         AS ProductName,
    Internal_Code        AS InternalCode,
    Main_Category        AS MainCategory,
    Sub_Category         AS SubCategory,
    Supplier_Info        AS SupplierInfo,
    Average_Retail_Price AS AverageRetailPrice,
    Total_Quantity_Sold  AS TotalQuantitySold,
    Total_Net_Revenue    AS TotalRevenue
FROM admin_WH.Vw_Sales(
    start_date = '2026-01-01',
    end_date = '2026-01-02',
    filter_by_pos = 0, -- ყველა სალარო
    pos_number = 0
)
ORDER BY TotalRevenue DESC;


---------------------------------------------------------


SELECT
    Operation_Date         AS OperationDate,
    Journal_ID             AS TransactionID,
    Barcode,
    Product_Name           AS ProductName,
    Main_Category          AS Category,

    -- ფინანსური ნაწილი
    Total_Revenue_Incl_VAT AS GrossRevenueInclVAT,
    Revenue_Excl_VAT       AS NetRevenueExclVAT,
    Product_Cost_Price     AS TotalCostPrice,
    Profit_Margin          AS NetProfitMargin,
    Quantity_Sold          AS QuantitySold,

    -- კონტრაგენტები
    Client_Info            AS ClientInfo,
    Supplier_Name          AS SupplierName,
    Account_Details        AS SupplierAccountDetails

FROM admin_WH.Vw_Profit_And_Margin(
    start_date = '2026-01-01',
    end_date = '2026-01-02'
)
ORDER BY NetProfitMargin DESC;



















