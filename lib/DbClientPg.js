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

			return this.raw.query ({
				text: sql,
				values: params,
			})

		}
		catch (x) {
		
			this.emit ('error', this, x)
		
		}
		finally {
		
			this.emit ('finish')
		
		}
	
	}

}

module.exports = DbClientPg