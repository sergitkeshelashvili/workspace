WITH userbr AS (
    SELECT b.*
    FROM ssa.UserBranches AS b
    INNER JOIN ssa.UMUxx AS u ON u.uxxid = b.uxxid
),

prods AS (
    SELECT
        pp.rec_type,
        pp.prodpp_id,
        pr.BCode,
        pro.Products_nu,
        pp.scount,
        pp.priceg,
        pr.incode,
        pp.vg,
        pp.opdt,
        cat.Category_nu,
        ppcat.PPCat_Nu,
        pp.status,
        pp.utypeh,
        r.batch_id,
        r.optype,
        r.rec_status,
        br.pos_num,
        bp.branch_id,
        bch.brname,
        concat(l.cr, ' - ', a.Acc_nu) AS cr
    FROM apos.POS_ReceiptProds AS pp
    INNER JOIN apos.POS_Receipts AS r ON pp.receipt_id = r.receipt_id
    INNER JOIN apos.POS_Batches AS br ON br.batch_id = r.batch_id
    INNER JOIN apos.POS_BookProp AS bp ON bp.pos_id = br.pos_num
    LEFT JOIN dbo.Branchs AS bch ON bch.br = bp.branch_id
    LEFT JOIN userbr AS ub ON bch.br = ub.brid
    LEFT JOIN ssa.UMUxx AS u ON u.uxxid = ub.uxxid AND isgrant = 1
    LEFT JOIN dbo.ProdPP AS pr ON pp.prodpp_id = pr.ProdPP_id
    LEFT JOIN Products AS pro ON pro.Products_id = pr.Products_id
    LEFT JOIN Category AS cat ON cat.Category_id = pro.Category_id
    LEFT JOIN PPCat ON ppcat.PPCat_ID = pr.PPCat_id
    LEFT JOIN tmpt.LastSuppData AS l ON l.prodpp_id = pp.prodpp_id
    LEFT JOIN Accounts AS a ON a.acc = l.cr
    WHERE
        pp.status = 4
        AND r.rec_status = 4
        AND (
            (pp.rec_type = 1 AND r.optype = 1)
            OR (pp.rec_type = 5 AND r.optype = 2)
        )
        AND br.opdate BETWEEN {d1:Date} AND {d2:Date}
        AND (ifNull({pos:Int32}, 0) = 0 OR br.pos_num = {pos:Int32})
        AND ({category_id:Nullable(Int32)} IS NULL OR pro.Category_id = {category_id:Int32})
        AND ({ppcat_id:Nullable(Int32)} IS NULL OR pr.ppcat_id = {ppcat_id:Int32})
        AND (
            {br:Nullable(Int32)} IS NULL
            OR br.pos_num IN (
                SELECT pos_id FROM apos.POS_BookProp WHERE branch_id = {br:Int32}
            )
        )
)

SELECT
    ifNull(BCode, toString(prodpp_id)) AS bcode,
    Products_nu,
    incode,
    Category_nu,
    PPCat_Nu,
    sum(scount)                        AS scount,
    priceg,
    sum(vg)                            AS total_vg,
    cr
FROM prods
GROUP BY
    ifNull(BCode, toString(prodpp_id)),
    Products_nu,
    Category_nu,
    PPCat_Nu,
    priceg,
    incode,
    cr
SETTINGS join_use_nulls = 1
