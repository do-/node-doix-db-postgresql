const {JobSource} = require ('doix')
const NOP = () => {}

class DbServicePg extends JobSource {

/*
	constructor (app, o = {}) {
*/
	test (notification) {

		return notification.isJSON

	}

	process (notification) {

		const job = this.createJob (notification.json)

		job.toComplete ().then (NOP, NOP)

	}

}

module.exports = DbServicePg