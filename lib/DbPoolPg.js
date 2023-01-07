const pg = require ('pg')
const {ResourcePool} = require ('doix')
const {DbEventLogger} = require ('doix-db')

const DbClientPg = require ('./DbClientPg.js')

class DbPoolPg extends ResourcePool {

	constructor (o) {

		super ()

		this.wrapper = DbClientPg

		this.pool = new pg.Pool (o.db)

		this.logger = o.logger

		this.eventLoggerClass = o.eventLoggerClass || DbEventLogger

	}

	async acquire () {

		return this.pool.connect ()

	}

}

module.exports = DbPoolPg