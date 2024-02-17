const { SQLService } = require('@cap-js/db-service')
const CQN2MSSQL = require('./CQN2MSSQL');
const cds = require('@sap/cds/lib')
const sql = require('mssql')
const $session = Symbol('dbc.session')

class MSSQLService extends SQLService {
  get factory() {
    return {
      options: { max: 1, ...this.options.pool },
      create: async tenant => {
        // TODO: tenant management
        const pool = new sql.ConnectionPool(this.options.credentials)
        await pool.connect()
        return pool
      },
      destroy: dbc => dbc.close(),
      validate: dbc => dbc.open,
    }
  }

  set(variables) {
    const dbc = this.dbc || cds.error('Cannot set session context: No database connection')
    if (!dbc[$session]) dbc[$session] = variables
    else Object.assign(dbc[$session], variables)
  }

  release() {
    this.dbc[$session] = undefined
    return super.release()
  }

  /// TRANSACTION MANAGEMENT
  async BEGIN() {
    const transaction = new sql.Transaction(this.dbc)
    await transaction.begin();

    this.dbc[$session].transaction = transaction
  }

  async COMMIT() {
    const transaction = this.dbc[$session].transaction
    if (transaction)
      await transaction.commit()
  }

  async ROLLBACK() {
    const transaction = this.dbc[$session].transaction
    if (transaction)
      await transaction.rollback()
  }

  /// PREPARE AND EXECUTE STATEMENT
  async prepare(sqlStatement) {
    // TODO: all logic for mssql goes here
    try {
      return {
        run: (..._) => this._run(sqlStatement, ..._),
        get: (..._) => this._get(sqlStatement, ..._),
        all: (..._) => this._all(sqlStatement, ..._),
        stream: (..._) => this._stream(sqlStatement, ..._),
      }
    } catch (e) {
      e.message += ' in:\n' + (e.sql = sqlStatement)
      throw e
    }
  }

  async _run(sqlStatement, binding_params) {
    var result = await this._internalExecute(sqlStatement, binding_params)
    return {
      changes: result?.rowsAffected[0]
    }
  }

  async _get(sqlStatement, binding_params) {
    // TODO: execute get statement
  }

  async _all(sqlStatement, binding_params) {
    var result = await this._internalExecute(sqlStatement, binding_params)
    return result?.recordset || []
  }

  async _stream(sqlStatement, binding_params, one) {
    // TODO: execute stream request
  }

  async exec(sqlCommand) {
    var commandFn = this[sqlCommand] || _internalExecute
    await commandFn.bind(this)(sqlCommand)
  }

  async _internalExecute(sqlStatement, binding_params) {
    // create request using transaction or pool
    const request = new sql.Request(this.dbc[$session]?.transaction || this.dbc);

    for (var i = 0; i < binding_params.length; i++)
      request.input(`p${i + 1}`, sql.VarChar(), binding_params[i])

    // execute query
    var result = await request.query(sqlStatement)
    return result
  }

  static CQN2SQL = CQN2MSSQL
}

module.exports = MSSQLService
