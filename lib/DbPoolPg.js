const pg = require ('pg')
const {ResourcePool} = require ('doix')

const DbClientPg = require ('./DbClientPg.js')

class DbPoolPg extends ResourcePool {

	constructor (o) {

		super ()

		this.wrapper = DbClientPg

		this.pool = new pg.Pool (o.db)

		this.logger = o.logger

		this.eventLoggerClass = o.eventLoggerClass || require ('./DbEventLoggerPg.js')

	}

	async acquire () {

		return this.pool.connect ()

	}

}

module.exports = DbPoolPg