const pg = require ('pg')
const {parse} = require ('pg-connection-string')
const {DbPool} = require ('doix-db')

const DbClientPg = require ('./DbClientPg.js')
const DbLangPg = require ('./DbLangPg.js')
const DbCallTrackerPg = require ('./DbCallTrackerPg.js')

const op = ({pool: {options}}) => {

	const {connectionString} = options

	return connectionString ? {...parse (connectionString), ...options} : options

}

const eq = (x, y) => {

	for (const k of ['user', 'database', 'host', 'port']) if (x [k] != y [k]) return false

	return true

}

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

	isSameDbAs (dbPoolPg) {

		return dbPoolPg instanceof DbPoolPg ? eq (op (this), op (dbPoolPg)) : false

	}

}

module.exports = DbPoolPg