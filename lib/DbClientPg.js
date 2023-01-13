const EventEmitter = require ('events')
const {randomUUID} = require ('crypto')

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
	
	async selectArray (sql, params = [], options = {}) {

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

}

module.exports = DbClientPg