const EventEmitter = require ('events')
const {randomUUID} = require ('crypto')
const PgQueryStream = require ('pg-query-stream')

const normalizeSQL = src => {

	const {length} = src

	let dst = '', pos = 0, n = 0
	
	while (pos < length) {
	
		const pq = src.indexOf ('?', pos)
		
		if (pq < 0) {
		
			dst += src.slice (pos)
		
			break
		
		}
		else {
			
			const s = src.slice (pos, pq)

			dst += s
			
			dst += s.trim ().slice (-7) === '::jsonb' ? '?' : '$' + (++ n)

			pos = pq + 1

		}

	}

	return dst

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

		const {rows} = await this.do (sql, params, options)

		if ('into' in options) {

			const {into} = options

			into.splice (into.length, 0, ...rows)

			return into

		}
		else {

			return rows

		}

	}

	async getStream (sql, params = [], options = {}) {

		sql = normalizeSQL (sql)

//		try {

			this.emit ('start', this, {sql, params})

	    	const stream = await this.raw.query (new PgQueryStream (sql, params))

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

}

DbClientPg.normalizeSQL = normalizeSQL

module.exports = DbClientPg