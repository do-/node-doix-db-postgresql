const pg = require ('pg')
const {DbEventLogger, DbPool} = require ('doix-db')

const DbClientPg = require ('./DbClientPg.js')
const DbLangPg = require ('./DbLangPg.js')

class DbPoolPg extends DbPool {

	constructor (o) {

		super (o)

		this.wrapper = DbClientPg

		this.pool = new pg.Pool (o.db)
		
		this.globals.lang = this.lang = o.lang || new DbLangPg ()

	}

	async acquire () {

		return this.pool.connect ()

	}

}

module.exports = DbPoolPg