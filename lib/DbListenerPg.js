//const EventEmitter = require ('events')
const {Router} = require ('doix')
const DbListenerLifeCycleTrackerPg = require ('./DbListenerLifeCycleTrackerPg')
const DbChannelPg = require ('./DbChannelPg')
const DbPoolPg = require ('./DbPoolPg')

class DbListenerPg extends Router {

	constructor (o) {
	
		super ()

		const {logger} = o; if (!logger) throw Error ('DbListenerPg: logger not set')

		this.pool = new DbPoolPg ({db: {max: 1, ...o.db}, logger})

		new DbListenerLifeCycleTrackerPg (this, logger)

	}

	async listen () {

		super.listen ()

		const process = msg => this.process (msg)

		const j = {on: _ => _, tracker: {prefix: this.uuid}}; await this.pool.toSet (j, 'db'); this.db = j.db

		this.db.raw.on ('notification', process)

		for (const destination of this.destinations) 
			
			if (destination instanceof DbChannelPg)
				
				await this.db.do ('LISTEN ' + this.db.lang.quoteName (destination.name))

		this.emit ('start')
	
	}

	async close () {

		await this.db.raw.release (true)

		this.emit ('finish')
	
	}

}

module.exports = DbListenerPg 