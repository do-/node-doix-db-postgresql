const {JobSource} = require ('doix')

const NOP = () => {}

class DbChannelPg extends JobSource {

	process (notification) {

		const job = this.createJob ()

		job.notification = notification

		job.toComplete ().then (NOP, NOP)

	}

}

module.exports = DbChannelPg