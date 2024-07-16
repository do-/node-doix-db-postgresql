const {JobSource} = require ('doix')

const OPEN = '{'.charCodeAt (0)
const NOP = () => {}
const RQ = Symbol ('rq')

class DbServicePg extends JobSource {

	test (notification) {

		if (notification == null) throw Error ('Empty notification')

		if (typeof notification !== 'object') throw Error ('Invalid notification: ' + notification)

		const {payload} = notification

		if (typeof payload !== 'string') throw Error ('Invalid notification payload: ' + payload)

		const {length} = payload; if (length === 0) throw Error ('Empty notification payload')

		if (length === 1) return false
				
		if (payload.charCodeAt (0) !== OPEN) return false

		try {
			
			notification [RQ] = JSON.parse (payload)

			return true

		} 
		catch (error) {

			return false
			
		}

	}

	process (notification) {

		const job = this.createJob ()

		job.notification = notification

		job.toComplete ().then (NOP, NOP)

	}

}

module.exports = DbServicePg