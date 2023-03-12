const EventEmitter  = require ('events')
const {randomUUID}  = require ('crypto')
const PgQueryStream = require ('pg-query-stream')
const PgCopyStream  = require ('pg-copy-streams')
const PgCursor      = require ('pg-cursor')
const {MAP_TYPE_ID_2_NAME} = require ('./DbLangPg.js')
const {DbQuery, DbRelation} = require ('doix-db')

class DbClientPg extends EventEmitter {

	constructor (raw) {
	
		super ()

		this.raw = raw
		
		this.uuid = randomUUID ()
	
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

	async getArray (q, p, o) {
	
		if (!(q instanceof DbQuery)) return this.getArrayOnly (q, p, o)
		
		const addCount = 'offset' in q.options, todo = addCount ? [null, this.getScalar (q.toQueryCount ())] : []
				
		const params = this.lang.toParamsSql (q), sql = params.pop (); todo [0] = this.getArrayOnly (sql, params, o)

		const done = await Promise.all (todo), rows = done [0]

		Object.defineProperty (rows, Symbol.for ('query'), {
			configurable: false,
			enumerable: false,
			get: () => q
		})

		if (addCount) {

			const count = parseInt (done [1]), get = () => count

			Object.defineProperty (rows, Symbol.for ('count'), {
				configurable: false,
				enumerable: false,
				get
			})

		}
		
		return rows
	
	}
	
	async getArrayOnly (sql, params = [], options = {}) {
	
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
	
	getObjectSQL (name, params) {
	
		const {model} = this; if (!model) return null

		const {map} = model; if (!map.has (name)) return null

		const {qName, pk, columns} = map.get (name)

		return `SELECT * FROM ${qName} WHERE ${pk.map (s => columns[s].qName + '=?').join (' AND ')}`

	}

	async getObject (sqlOrName, params = [], options = {}) {
	
		let sql = this.getObjectSQL (sqlOrName, params)

		if (sql === null) {
		
			sql = sqlOrName
		
		}
		else {
		
			if (!Array.isArray (params)) params = [params]

		}

		const a = await this.getArray (sql, params, {
			...options,
			maxRows: 1,		
			isPartial: true,
		})
		
		if (a.length === 1) return a [0]
	
		const {notFound} = options; if (notFound instanceof Error) throw notFound
		
		return 'notFound' in options ? notFound : {}

	}

	async getScalar (sql, params = [], options = {}) {

		return this.getObject (sql, params, {
			notFound: undefined,
			...options,
			rowMode: 'scalar',
		})
		
	}

}

module.exports = DbClientPg