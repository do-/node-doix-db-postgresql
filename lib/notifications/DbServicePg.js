const {Router: {TEST_MESSAGE, PROCESS_MESSAGE}} = require ('protocol-agnostic-router')
const {JobSource} = require ('doix')
const NOP = () => {}

class DbServicePg extends JobSource {

	constructor (app, o) {

		super (app, o)

		if ('test' in o) {

			this [TEST_MESSAGE] = o.test

		}
		else {

			this [TEST_MESSAGE] = n => n.isJSON

		}

	}

	[PROCESS_MESSAGE] (notification) {

		const job = this.createJob (notification.json)

		job.outcome ().then (NOP, NOP)

	}

}

module.exports = DbServicePg