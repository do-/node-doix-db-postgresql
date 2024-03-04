const pg = require ('pg')
const {DbPool} = require ('doix-db')

const DbClientPg = require ('./DbClientPg.js')
const DbLangPg = require ('./DbLangPg.js')
const DbCallTrackerPg = require ('./DbCallTrackerPg.js')

class DbPoolPg extends DbPool {

	constructor (o) {

		super ({trackerClass: DbCallTrackerPg, ...o})

		this.wrapper = DbClientPg

		this.pool = new pg.Pool (o.db)
		
		this.lang = o.lang || new DbLangPg ()

	}

	async acquire () {

		return this.pool.connect ()

	}

}

module.exports = DbPoolPg