const EventEmitter  = require ('events')
const {randomUUID}  = require ('crypto')
const PgQueryStream = require ('pg-query-stream')
const PgCursor      = require ('pg-cursor')
const {MAP_TYPE_ID_2_NAME} = require ('./DbLangPg.js')
const {DbQuery}     = require ('doix-db')

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

	async getArray (q, p, o) {
	
		if (!(q instanceof DbQuery)) return this.getArrayOnly (q, p, o)
		
		const addCount = 'offset' in q.options, todo = addCount ? [null, this.getScalar (q.toQueryCount ())] : []
				
		const params = this.lang.toParamsSql (q), sql = params.pop (); todo [0] = this.getArrayOnly (sql, params, o)

		const done = await Promise.all (todo), rows = done [0]

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

	async getObject (sql, params = [], options = {}) {
		
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