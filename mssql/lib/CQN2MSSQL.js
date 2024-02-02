const { SQLService } = require('@cap-js/db-service')

class CQN2MSSQL extends SQLService.CQN2SQL {

    // override select for MSSQL
    SELECT(q) {
        const _empty = a => !a || a.length === 0
        const _limit = ({ rows, offset }) => {
            if (!rows) throw new Error('Rows parameter is missing in SELECT.limit(rows, offset)')
            return `OFFSET ${offset?.val || 0} ROWS FETCH NEXT ${rows.val} ROWS ONLY`
        }

        let { from, expand, where, groupBy, having, orderBy, limit, one, distinct, localized } = q.SELECT
        // REVISIT: When selecting from an entity that is not in the model the from.where are not normalized (as cqn4sql is skipped)
        if (!where && from?.ref?.length === 1 && from.ref[0]?.where) where = from.ref[0]?.where
        let columns = this.SELECT_columns(q)
        let sql = `SELECT`
        if (distinct) sql += ` DISTINCT`
        if (!_empty(columns)) sql += ` ${columns}`
        if (!_empty(from)) sql += ` FROM ${this.from(from)}`
        if (!_empty(where)) sql += ` WHERE ${this.where(where)}`
        if (!_empty(groupBy)) sql += ` GROUP BY ${this.groupBy(groupBy)}`
        if (!_empty(having)) sql += ` HAVING ${this.having(having)}`
        if (!_empty(orderBy)) sql += ` ORDER BY ${this.orderBy(orderBy, localized)}`
        if (one) limit = Object.assign({}, limit, { rows: { val: 1 } })
        if (limit) sql += ` ${_limit(limit)}`
        // Expand cannot work without an inferred query
        if (expand) {
            if ('elements' in q) sql = this.SELECT_expand(q, sql)
            else cds.error`Query was not inferred and includes expand. For which the metadata is missing.`
        }
        return (this.sql = sql)
    }

    // override select_expand (mssql json_object fixes)
    SELECT_expand(q, sql) {
        if (!('elements' in q)) return sql

        const SELECT = q.SELECT
        if (!SELECT.columns) return sql

        let cols = SELECT.columns.map(x => {
            const name = this.column_name(x)
            let col = `'${name}':r.${this.output_converter4(x.element, this.quote(name))}`
            if (x.SELECT?.count) {
                // Return both the sub select and the count for @odata.count
                const qc = cds.ql.clone(x, { columns: [{ func: 'count' }], one: 1, limit: 0, orderBy: 0 })
                return [col, `'${name}@odata.count',${this.expr(qc)}`]
            }
            return col
        }).flat()

        // Prevent MSSQL from hitting function argument limit of 100
        let obj = ''

        if (cols.length < 50) obj = `json_object(${cols.slice(0, 50)})`
        else {
            const chunks = []
            for (let i = 0; i < cols.length; i += 50) {
                chunks.push(`json_object(${cols.slice(i, i + 50)})`)
            }
            // REVISIT: json_merge is a user defined function, bad performance!
            obj = `json_merge(${chunks})`
        }


        return `SELECT ${SELECT.one || SELECT.expand === 'root' ? obj : `json_group_array(${obj.includes('json_merge') ? `json_insert(${obj})` : obj})`} as _json_ FROM (${sql}) as r`
    }
}

module.exports = CQN2MSSQL;