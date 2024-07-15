const {Router} = require ('doix')
const DbListenerLifeCycleTrackerPg = require ('./DbListenerLifeCycleTrackerPg')
const DbChannelPg = require ('./DbChannelPg')
const DbPoolPg = require ('./DbPoolPg')

class DbListenerPg extends Router {

	constructor (o) {
	
		super ()
/*
		const {channel} = o

		if (!channel) throw Error ('DbListenerPg: channel not set')
		
		if (typeof channel !== 'string') throw Error ('DbListenerPg: channel must be set as a string, found ' + channel)

		this.channel = channel
*/
		const {logger} = o; if (!logger) throw Error ('DbListenerPg: logger not set')

		this.pool = new DbPoolPg ({db: {max: 1, ...o.db}, logger})

		this.tracker = new DbListenerLifeCycleTrackerPg (this, logger)

	}

	process (msg) {
		
		this.emit ('notification', msg)
		
		super.process (msg)

	}

	async listen () {

		super.listen ()

		await this.pool.toSet (this, 'db')

		const {db} = this

		db.raw.on ('notification', msg => this.process (msg))

		for (const destination of this.destinations) 
			
			if (destination.name /*instanceof DbChannelPg*/)
				
				await db.do ('LISTEN ' + this.db.lang.quoteName (destination.name))

		this.emit ('start')
	
	}

	async close () {

		await new Promise (ok => {

//			this.pool.pool.on ('release', (_, client) => {if (client === this.db.raw) ok ()})
			this.pool.pool.on ('release', ok)

			this.emit ('finish')

		})

		await this.pool.pool.end ()

	}

}

module.exports = DbListenerPg 