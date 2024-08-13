const {Router}         = require ('doix')
const DbPoolPg         = require ('../DbPoolPg')
const DbNotificationPg = require ('./DbNotificationPg')
const {Tracker}        = require ('events-to-winston')

class DbListenerPg extends Router {

	constructor (o) {

		const {channel, logger} = o

		if (!channel) throw Error ('DbListenerPg: channel not set')

		if (typeof channel !== 'string') throw Error ('DbListenerPg: channel must be set as a string, found ' + channel)

		o.name ??= channel
	
		super (o)

		this.channel = channel

		this.pool = new DbPoolPg ({db: {max: 1, ...o.db}, logger})

	}

	get [Tracker.LOGGING_EVENTS] () {

		const e = super [Tracker.LOGGING_EVENTS]

		e.start.details = function () {

			const {user, host, port, database} = this.db.raw.connectionParameters

			return {user, host, port, database}

		}

		e.notification = {
			level: 'info',
			message: n => n.payload,
			details: n => ({processId: n.processId}),
		}

		return e

	}

	process (raw) {

		const n = new DbNotificationPg (raw)
		
		this.emit ('notification', n)
		
		super.process (n)

	}

	async listen () {

		super.listen ()

		await this.pool.toSet (this, 'db')

		const {db} = this

		db.raw.on ('notification', msg => this.process (msg))

		await db.do ('LISTEN ' + this.db.lang.quoteName (this.channel))

		this.emit ('start')
	
	}

	async close () {

		await new Promise (ok => {

			this.pool.pool.on ('release', ok)

			this.emit ('finish')

		})

		await this.pool.pool.end ()

	}

}

module.exports = DbListenerPg 