const pg = require ('pg')
const {DbEventLogger, DbPool} = require ('doix-db')

const DbClientPg = require ('./DbClientPg.js')

class DbPoolPg extends DbPool {

	constructor (o) {

		super (o)

		this.wrapper = DbClientPg

		this.pool = new pg.Pool (o.db)

	}

	async acquire () {

		return this.pool.connect ()

	}

}

module.exports = DbPoolPg