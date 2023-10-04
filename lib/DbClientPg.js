const {randomUUID}  = require ('crypto')
const PgQueryStream = require ('pg-query-stream')
const PgCopyStream  = require ('pg-copy-streams')
const PgCursor      = require ('pg-cursor')
const {MAP_TYPE_ID_2_NAME} = require ('./DbLangPg.js')
const {DbClient, DbQuery, DbRelation} = require ('doix-db')
const DbTableXformerPg = require ('./DbTableXformerPg.js')

class DbClientPg extends DbClient {

	constructor (raw) {
	
		super ()

		this.raw = raw
	
	}
	
	async release () {
	
		this.raw.release ()
	
	}
	
	async do (sql, params = [], options = {}) {
	
		sql = this.lang.normalizeSQL (sql)
	
		try {

			this.emit ('start', this, {sql, params})

			return await this.raw.query ({
				text: sql,
				values: params,
			})

		}
		catch (cause) {
		
			this.emit ('error', this, cause)
			
			throw Error ('PostgreSQL server returned an error: ' + cause.message, {cause})
		
		}
		finally {
		
			this.emit ('finish')
		
		}
	
	}
	
	async insert (name, data, options = {}) {
			
		const params = this.lang.genInsertParamsSql (name, data, options)
		
		const sql = params.pop ()
		
		await this.do (sql, params)

	}

	async upsert (name, data, options) {

		const params = this.lang.genUpsertParamsSql (name, data, options)
		
		const sql = params.pop ()
		
		await this.do (sql, params)

	}

	async update (name, data, options = {}) {
			
		const params = this.lang.genUpdateParamsSql (name, data, options)
		
		if (params === null) return
		
		const sql = params.pop ()
		
		await this.do (sql, params)

	}
	
	async getArrayBySql (sql, params = [], options = {}) {
	
		const maxRows = options.maxRows || 1000
		const isPartial = options.isPartial === true

		sql = this.lang.normalizeSQL (sql)
		
		if (!Number.isInteger (maxRows)) throw Error ('maxRows must be an integer number, not ' + maxRows)

		if (maxRows <= 0) throw Error ('maxRows must be positive, not ' + maxRows)

		const o = {}; if ('rowMode' in options) switch (options.rowMode) {
		
			case 'array':
			case 'scalar':
				o.rowMode = 'array'
				break
				
			default:
				throw Error ('Invalid row mode: ' + options.rowMode)
		
		}

		this.emit ('start', this, {sql, params})

		return new Promise ((ok, fail) => {

			const cursor = this.raw.query (new PgCursor (sql, params, o))

			cursor.read (isPartial ? maxRows : maxRows + 1, (cause, rows) => {

				cursor.close ()
				
				if (!isPartial && rows && rows.length > maxRows) cause = Error (maxRows + ' rows limit exceeded. Plesae fix the request or consider using getStream instead of getArray')

				if (cause) {

					this.emit ('error', this, cause)
					
					fail (Error ('PostgreSQL server returned an error: ' + cause.message, {cause}))
			
				}
				else {
				
					if (options.rowMode === 'scalar') 
					
						for (let i = 0; i < rows.length; i ++)
						
							rows [i] = rows [i] [0]
														
					Object.defineProperty (rows, Symbol.for ('columns'), {
						configurable: false,
						enumerable: false,
						get: () => cursor._result.fields.map (i => {i.type = MAP_TYPE_ID_2_NAME.get (i.dataTypeID); return i})
					})

					ok (rows)
				
				}

			})
		
		}).finally (() => this.emit ('finish'))

	}

	async putStream (name, columns, options = {}) {

		const sql = this.lang.genCopyFromSql (name, columns, options)

		this.emit ('start', this, {sql, params: []})

		const stream = await this.raw.query (PgCopyStream.from (sql))

		stream.on ('error', cause => this.emit ('error', this, cause))

		stream.on ('finish', () => this.emit ('finish'))

		return stream

	}

	async getStream (sql, params = [], options = {}) {

		if (sql instanceof DbQuery) {
		
			params = this.lang.toParamsSql (sql)
			
			sql = params.pop ()
		
		}

		sql = this.lang.normalizeSQL (sql)

//		try {

			this.emit ('start', this, {sql, params})
			
			const o = {}; if ('rowMode' in options) o.rowMode = options.rowMode

	    	const stream = await this.raw.query (new PgQueryStream (sql, params, o))

			stream.on ('error', cause => this.emit ('error', this, cause))

			stream.on ('end', () => this.emit ('finish'))

			return stream
/*
		}
		catch (cause) {

			this.emit ('error', this, cause)

			throw Error ('PostgreSQL server returned an error: ' + cause.message, {cause})

		}
*/
	}
	
	async getStreamOfExistingTables () {

		const {defaultSchema} = this.model

		if (!defaultSchema.schemaName) defaultSchema.schemaName = await this.getScalar ('SELECT CURRENT_SCHEMA ()')

		const rs = await this.getStream (this.lang.genSelectColumnsSql ())
		
		const xform = new DbTableXformerPg (this.model)
		
		rs.on ('error', x => xform.destroy (x))
		
		return rs.pipe (xform)

	}

}

module.exports = DbClientPg