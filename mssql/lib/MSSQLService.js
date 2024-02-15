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
    await this._internalExecute(sqlCommand, [])
  }

  async _internalExecute(sqlStatement, binding_params) {
    try {
      // prepare sql statement
      const stmt = new sql.PreparedStatement()
      var parameters = {};

      for (var i = 0; i < binding_params.length; i++) {
        var parameterKey = `p${i + 1}`

        stmt.input(parameterKey, sql.VarChar())
        parameters[parameterKey] = binding_params[i]
      }

      await stmt.prepare(sqlStatement)

      // execute query
      var result = await stmt.execute(parameters);

      // unprepare and return
      await stmt.unprepare()
      return result;

    }
    catch (err) {
      // error handling
      return {}
    }
  }

  static CQN2SQL = CQN2MSSQL
}

module.exports = MSSQLService
