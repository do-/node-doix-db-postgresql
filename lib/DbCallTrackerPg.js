const {DbCallTracker} = require ('doix-db')

class DbCallTrackerPg extends DbCallTracker {

	/*
	constructor (call) {

		const {db} = call

		super (call, db.pool.logger)

		this.call = call

		this.prefix = db.job.tracker.prefix + '/' + db.uuid + '/' + call.ord

	}
	*/

	toString ({message, where}) {

		return where ? `${message} (${where})` : message

	}

	noticeMessage (m) {

		return {
			message: this.toString (m),
			level: 'notice'
		}

	}

}

module.exports = DbCallTrackerPg