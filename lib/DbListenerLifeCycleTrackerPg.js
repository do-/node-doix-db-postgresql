const {LifeCycleTracker} = require ('doix')

class DbListenerLifeCycleTrackerPg extends LifeCycleTracker {

	constructor (router, logger) {

		super (router, logger)

		this.router = router

		this.prefix = router.uuid

		this.postfix = ' listening for DB notifications'

	}

	startMessage () {

		const {user, host, port, database, processID} = this.router.db.raw.connectionParameters

		return super.startMessage () + ` listening for DB notifications at ${user}@${host}:${port}/${database} (PID ${processID})`

	}

}

module.exports = DbListenerLifeCycleTrackerPg