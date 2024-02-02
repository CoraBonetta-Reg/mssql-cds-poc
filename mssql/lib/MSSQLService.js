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
        const dbc = await sql.connect(this.options.credentials)
        return dbc
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

  async prepare(sqlStatement) {
    // TODO: all logic for mssql goes here
    try {
      const stmt = new sql.PreparedStatement()
      await stmt.prepare(sqlStatement)

      return {
        run: (..._) => this._run(stmt, ..._),
        get: (..._) => this._get(stmt,..._),
        all: (..._) => this._all(stmt,..._),
        stream: (..._) => this._stream(stmt, ..._),
      }
    } catch (e) {
      e.message += ' in:\n' + (e.sql = sql)
      throw e
    }
  }

  async _run(stmt, binding_params) {
    // TODO: execute run statement
  }
  
  async _get(stmt, binding_params) {
    // TODO: execute get statement
  }

  async _all(stmt, binding_params) {
    var result = await stmt.execute();
    return result?.recordset || [];
  }

  async _stream(stmt, binding_params, one) {
    // TODO: execute stream request
  }

  async exec(sqlCommand) {
    const ps = new sql.PreparedStatement()
    await ps.prepare()
    return await ps.execute(sqlCommand)
  }

  static CQN2SQL = CQN2MSSQL
}

module.exports = MSSQLService
