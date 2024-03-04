const {JobSource} = require ('doix')

const NOP = () => {}

class DbChannelPg extends JobSource {

	constructor (app, o) {

		if (!('name' in o)) throw Error ('DbChannelPg: name not set')

		const {name} = o

		if (typeof name !== 'string') throw Error ('DbChannelPg: name must be a string')

		if (name.length === 0) throw Error ('DbChannelPg: name must be a non empty string')

		super (app, o)

		this.name = name

		this.test = ({channel}) => channel === name
		
	}

	process (notification) {

		const job = this.app.createJob ()

		job.notification = notification
				
		this.copyHandlersTo (job)

		job.toComplete ().then (NOP, NOP)

	}

}

module.exports = DbChannelPg