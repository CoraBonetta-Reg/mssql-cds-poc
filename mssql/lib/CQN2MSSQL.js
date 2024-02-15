const { SQLService } = require('@cap-js/db-service')

// const remapping from base service
const ObjectKeys = o => (o && [...ObjectKeys(o.__proto__), ...Object.keys(o)]) || []
const _managed = {
  '$user.id': '$user.id',
  $user: '$user.id',
  $now: '$now',
}

const is_regexp = x => x?.constructor?.name === 'RegExp' // NOTE: x instanceof RegExp doesn't work in repl
const _empty = a => !a || a.length === 0

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
        if (one) sql += ` TOP 1`
        if (!_empty(columns)) sql += ` ${columns}`
        if (!_empty(from)) sql += ` FROM ${this.from(from)}`
        if (!_empty(where)) sql += ` WHERE ${this.where(where)}`
        if (!_empty(groupBy)) sql += ` GROUP BY ${this.groupBy(groupBy)}`
        if (!_empty(having)) sql += ` HAVING ${this.having(having)}`
        if (!_empty(orderBy)) sql += ` ORDER BY ${this.orderBy(orderBy, localized)}`
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

    // override value rendering for parameters in statement
    val({ val, param }) {
        switch (typeof val) {
          case 'function': throw new Error('Function values not supported.')
          case 'undefined': return 'NULL'
          case 'boolean': return `${val}`
          case 'number': return `${val}` // REVISIT for HANA
          case 'object':
            if (val === null) return 'NULL'
            if (val instanceof Date) return `'${val.toISOString()}'`
            if (val instanceof Readable); // go on with default below
            else if (Buffer.isBuffer(val)) val = val.toString('base64')
            else if (is_regexp(val)) val = val.source
            else val = JSON.stringify(val)
          case 'string': // eslint-disable-line no-fallthrough
        }
        if (!this.values || param === false) 
            return this.string(val)

        var index = this.values.push(val)
        return `@p${index}`
      }

    // insert entries override
    INSERT_entries(q) {
        const { INSERT } = q
        const entity = this.name(q.target?.name || INSERT.into.ref[0])
        const alias = INSERT.into.as
        const elements = q.elements || q.target?.elements
        if (!elements && !INSERT.entries?.length) {
          return // REVISIT: mtx sends an insert statement without entries and no reference entity
        }
        const columns = elements
          ? ObjectKeys(elements).filter(c => c in elements && !elements[c].virtual && !elements[c].value && !elements[c].isAssociation)
          : ObjectKeys(INSERT.entries[0])
    
        /** @type {string[]} */
        this.columns = columns.filter(elements ? c => !elements[c]?.['@cds.extension'] : () => true).map(c => this.quote(c))
    
        const extractions = this.managed(
          columns.map(c => ({ name: c })),
          elements,
          !!q.UPSERT,
        )

        const extraction = extractions
          .filter(a => a)
          .map(c => `${c.name} ${c.type} '${c.sql}'`)
          .join(',')

        this.entries = [[...this.values, JSON.stringify(INSERT.entries)]]
        return (this.sql = `INSERT INTO ${this.quote(entity)}${alias ? ' as ' + this.quote(alias) : ''} (${this.columns
          }) SELECT * FROM OPENJSON(@p1) WITH (${extraction})`)
      }

      // managed override, json syntax is a bit different during insert/update
      managed(columns, elements, isUpdate = false) {
        const annotation = isUpdate ? '@cds.on.update' : '@cds.on.insert'
        const { _convertInput } = this.class
        // Ensure that missing managed columns are added
        const requiredColumns = !elements
          ? []
          : Object.keys(elements)
            .filter(
              e =>
                (elements[e]?.[annotation] || (!isUpdate && elements[e]?.default && !elements[e].virtual && !elements[e].isAssociation)) &&
                !columns.find(c => c.name === e),
            )
            .map(name => ({ name, sql: 'NULL' }))
    
        return [...columns, ...requiredColumns].map(({ name, sql }) => {
          let element = elements?.[name] || {}
          if (!sql) sql = `$.${name}`
    
          let converter = element[_convertInput]
          if (converter && sql[0] !== '$') sql = converter(sql, element)
    
          let val = _managed[element[annotation]?.['=']]
          if (val) sql = `coalesce(${sql}, ${this.func({ func: 'session_context', args: [{ val, param: false }] })})`
          else if (!isUpdate && element.default) {
            const d = element.default
            if (d.val !== undefined || d.ref?.[0] === '$now') {
              // REVISIT: d.ref is not used afterwards
              sql = `(CASE WHEN json_type(value,'$."${name}"') IS NULL THEN ${this.defaultValue(d.val) // REVISIT: this.defaultValue is a strange function
                } ELSE ${sql} END)`
            }
          }
          const type = this.type4(element)
          return { name, sql, type }
        })
      }

      // types determination
      static TypeMap = {
        // Utilizing cds.linked inheritance
        UUID: e => `NVARCHAR(36)`,
        String: e => `NVARCHAR(${e.length || 'MAX'})`,
        Binary: e => `VARBINARY(${e.length || 'MAX'})`,
        Int64: () => 'BIGINT',
        Int32: () => 'INTEGER',
        Int16: () => 'SMALLINT',
        UInt8: () => 'SMALLINT',
        Integer64: () => 'BIGINT',
        LargeString: () => 'NCLOB',
        LargeBinary: () => 'BLOB',
        Association: () => false,
        Composition: () => false,
        array: () => 'NCLOB',
      }

      type4(element) {
        if (!element._type) element = cds.builtin.types[element.type] || element
        const fn = element[this.class._sqlType]
        return (
          fn?.(element) || element._type?.replace('cds.', '').toUpperCase() || cds.error`Unsupported type: ${element.type}`
        )
      }
}

module.exports = CQN2MSSQL;