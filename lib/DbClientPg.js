const EventEmitter  = require ('events')
const {randomUUID}  = require ('crypto')
const PgQueryStream = require ('pg-query-stream')
const PgCursor      = require ('pg-cursor')

const normalizeSQL = src => {

	const CH_QUEST = '?'.charAt (0)
	const CH_QUOTE = "'".charAt (0)
	const CH_SLASH = '/'.charAt (0)
	const CH_ASTER = '*'.charAt (0)
	const CH_MINUS = '-'.charAt (0)
	
	const ST_SQL     = 0
	const ST_LITERAL = 1
	const ST_COMMENT = 2

	const {length} = src
	
	let n = 0, dst = '', last = 0, next = -1, depth = 0, state = ST_SQL
	
	while (next < length) {
	
		next ++; const c = src.charAt (next)

		switch (state) {

			case ST_LITERAL:
				if (c === CH_QUOTE && src.charAt (next + 1) !== CH_QUOTE) state = ST_LITERAL
				break

			case ST_SQL:

				switch (c) {

					case CH_QUEST:
						const s = src.slice (last, next)
						dst += s
						dst += s.trim ().slice (-7) === '::jsonb' ? '?' : '$' + (++ n)
						last = next + 1
						break

					case CH_QUOTE:
						state = ST_LITERAL
						break

					case CH_SLASH:
						if (src.charAt (next + 1) !== CH_ASTER) break
						dst += src.slice (last, next)
						state = ST_COMMENT
						depth = 1
						next ++
						break

					case CH_MINUS:
						if (src.charAt (next + 1) !== CH_MINUS) break
						dst += src.slice (last, next)
						last = src.indexOf ('\n', next)
						if (last < 0) return dst
						next = last
						break

				}
				break

			case ST_COMMENT:

				if (c !== CH_SLASH) break				

				if (src.charAt (next - 1) === CH_ASTER) {
					depth --
					if (depth > 0) break
					state = ST_SQL
					last = next + 1
				}
				else if (src.charAt (next + 1) === CH_ASTER) {
					depth ++
				}
				break

		}
	
	}
	
	return dst + src.slice (last)

}

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
	
		sql = normalizeSQL (sql)
	
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
	
	async getArray (sql, params = [], options = {}) {
				
		if ('limit' in options) {
		
			const limit = parseInt (options.limit)

			if (!Number.isInteger (limit)) throw Error ('limit must be an integer number, not ' + limit)

			if (limit <= 0) throw Error ('limit must be positive, not ' + limit)
			
			sql += ' LIMIT ?'; params.push (limit)
			
			options.maxRows = limit
			options.isPartial = true

		}

		const maxRows = options.maxRows || 1000
		const isPartial = options.isPartial === true

		if ('offset' in options) {
			
			if (!('limit' in options)) throw Error ('offset cannot be set without limit')

			const offset = parseInt (options.offset)

			if (!Number.isInteger (offset)) throw Error ('offset must be an integer number, not ' + offset)

			if (offset < 0) throw Error ('offset cannot be nagative, as ' + offset)

			sql += ' OFFSET ?'; params.push (offset)

		}
		
		sql = normalizeSQL (sql)
		
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
				
					ok (rows)
				
				}

			})
		
		}).finally (() => this.emit ('finish'))

	}

	async getStream (sql, params = [], options = {}) {

		sql = normalizeSQL (sql)

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

DbClientPg.normalizeSQL = normalizeSQL

module.exports = DbClientPg