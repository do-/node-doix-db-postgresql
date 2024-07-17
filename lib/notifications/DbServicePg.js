const {JobSource} = require ('doix')
const NOP = () => {}

class DbServicePg extends JobSource {

	constructor (app, o = {}) {

		super (app, o)

		if ('test' in o) {

			this.test = o.test

		}
		else {

			this.test = n => n.isJSON

		}

	}

	process (notification) {

		const job = this.createJob (notification.json)

		job.toComplete ().then (NOP, NOP)

	}

}

module.exports = DbServicePg